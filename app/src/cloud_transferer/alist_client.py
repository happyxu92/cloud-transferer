"""AList HTTP API 客户端封装。

只覆盖本项目用到的接口：
- 登录
- 列目录 / 元信息 / 建目录
- 复制文件（异步任务） + 任务轮询 + 任务清理

参考: https://alist.nn.ci/zh/guide/api/
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class AListError(Exception):
    def __init__(self, code: int, message: str, payload: Any = None) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.payload = payload


@dataclass
class FileEntry:
    name: str
    path: str          # 绝对 AList 路径
    size: int
    is_dir: bool
    modified: str | None = None
    sign: str | None = None


@dataclass
class CopyTask:
    id: str
    name: str
    state: int          # AList task state: 0 pending, 1 running, 2 success, 7 failed ...
    progress: float
    error: str | None = None


# AList 任务状态码（来自 alist/internal/task）
TASK_STATE_PENDING = 0
TASK_STATE_RUNNING = 1
TASK_STATE_SUCCEEDED = 2
TASK_STATE_CANCELING = 3
TASK_STATE_CANCELED = 4
TASK_STATE_ERRORED = 5
TASK_STATE_FAILING = 6
TASK_STATE_FAILED = 7
TASK_STATE_WAITING = 8
TASK_STATE_BEFORE = 9

FINISHED_STATES = {
    TASK_STATE_SUCCEEDED,
    TASK_STATE_CANCELED,
    TASK_STATE_FAILED,
    TASK_STATE_ERRORED,
}
SUCCESS_STATES = {TASK_STATE_SUCCEEDED}


class AListClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self._token: str | None = None
        self._token_ts: float = 0.0
        self._client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    # -------------------- low level --------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
        auth: bool = True,
    ) -> Any:
        if auth:
            await self._ensure_token()
        headers = {}
        if auth and self._token:
            headers["Authorization"] = self._token
        url = f"{self.base_url}{path}"
        r = await self._client.request(
            method, url, json=json, params=params, headers=headers
        )
        r.raise_for_status()
        data = r.json()
        code = data.get("code")
        if code != 200:
            # token 过期，重新登录一次
            if auth and code in (401, 402):
                logger.warning("AList token 过期，重新登录")
                self._token = None
                await self._ensure_token()
                headers["Authorization"] = self._token or ""
                r = await self._client.request(
                    method, url, json=json, params=params, headers=headers
                )
                r.raise_for_status()
                data = r.json()
                if data.get("code") == 200:
                    return data.get("data")
            raise AListError(code or -1, data.get("message", "unknown"), data)
        return data.get("data")

    async def _ensure_token(self) -> None:
        # token 默认 48h，这里 6h 主动刷一次保险
        if self._token and (time.time() - self._token_ts) < 6 * 3600:
            return
        data = await self._request(
            "POST",
            "/api/auth/login",
            json={"username": self.username, "password": self.password},
            auth=False,
        )
        self._token = data["token"]
        self._token_ts = time.time()
        logger.info("AList 登录成功")

    # -------------------- file ops --------------------

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError,)),
        wait=wait_exponential(min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def list_dir(
        self,
        path: str,
        *,
        password: str = "",
        page: int = 1,
        per_page: int = 0,  # 0 = all
        refresh: bool = False,
    ) -> list[FileEntry]:
        data = await self._request(
            "POST",
            "/api/fs/list",
            json={
                "path": path,
                "password": password,
                "page": page,
                "per_page": per_page,
                "refresh": refresh,
            },
        )
        content = (data or {}).get("content") or []
        result: list[FileEntry] = []
        for it in content:
            name = it["name"]
            full = path.rstrip("/") + "/" + name if path != "/" else "/" + name
            result.append(
                FileEntry(
                    name=name,
                    path=full,
                    size=int(it.get("size") or 0),
                    is_dir=bool(it.get("is_dir")),
                    modified=it.get("modified"),
                    sign=it.get("sign"),
                )
            )
        return result

    async def list_dir_recursive(
        self, path: str, *, refresh: bool = False
    ) -> list[FileEntry]:
        """递归 BFS 列出全部文件（仅文件，不含目录）。"""
        out: list[FileEntry] = []
        stack = [path]
        visited: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            try:
                entries = await self.list_dir(cur, refresh=refresh)
            except AListError as e:
                logger.error(f"列目录失败 {cur}: {e}")
                continue
            for e in entries:
                if e.is_dir:
                    stack.append(e.path)
                else:
                    out.append(e)
            # 轻微速率限制，避免触发百度风控
            await asyncio.sleep(0.3)
        return out

    async def get(self, path: str, *, password: str = "") -> dict | None:
        try:
            return await self._request(
                "POST",
                "/api/fs/get",
                json={"path": path, "password": password},
            )
        except AListError as e:
            if "object not found" in (e.message or "").lower() or e.code in (
                500,
                404,
            ):
                return None
            raise

    async def mkdir(self, path: str) -> None:
        try:
            await self._request("POST", "/api/fs/mkdir", json={"path": path})
        except AListError as e:
            # 已存在视为成功
            if "exist" in (e.message or "").lower():
                return
            raise

    async def ensure_dir(self, path: str) -> None:
        """递归确保 path 存在。"""
        parts = [p for p in path.split("/") if p]
        cur = ""
        for p in parts:
            cur = f"{cur}/{p}"
            await self.mkdir(cur)

    async def copy(
        self, src_dir: str, dst_dir: str, names: list[str]
    ) -> list[str]:
        """触发复制，返回 AList task id 列表（每个文件一个 task）。"""
        data = await self._request(
            "POST",
            "/api/fs/copy",
            json={"src_dir": src_dir, "dst_dir": dst_dir, "names": names},
        )
        # 不同 AList 版本返回结构略有差异
        # 新版返回 {"tasks": [{"id": "...", "name": "..."}]}
        # 老版可能直接返回 None；保底用 list/done 接口找回
        ids: list[str] = []
        if isinstance(data, dict) and "tasks" in data:
            ids = [t["id"] for t in data.get("tasks", []) if t.get("id")]
        return ids

    async def remove(self, dir_path: str, names: list[str]) -> None:
        await self._request(
            "POST",
            "/api/fs/remove",
            json={"dir": dir_path, "names": names},
        )

    # -------------------- task ops --------------------

    async def _list_tasks(self, kind: str) -> list[CopyTask]:
        """kind in {undone, done}"""
        data = await self._request("GET", f"/api/admin/task/copy/{kind}")
        out: list[CopyTask] = []
        for it in data or []:
            out.append(
                CopyTask(
                    id=str(it.get("id")),
                    name=it.get("name", ""),
                    state=int(it.get("state", 0)),
                    progress=float(it.get("progress", 0) or 0),
                    error=it.get("error") or None,
                )
            )
        return out

    async def list_undone_copy(self) -> list[CopyTask]:
        return await self._list_tasks("undone")

    async def list_done_copy(self) -> list[CopyTask]:
        return await self._list_tasks("done")

    async def delete_copy_task(self, tid: str) -> None:
        try:
            await self._request(
                "POST", "/api/admin/task/copy/delete", params={"tid": tid}
            )
        except AListError as e:
            logger.debug(f"删除任务 {tid} 失败（可忽略）: {e}")

    async def wait_task(
        self, task_id: str, *, timeout_sec: int, poll_interval: float = 5.0
    ) -> CopyTask:
        """轮询单个 task 直到完成。"""
        deadline = time.time() + timeout_sec
        last: CopyTask | None = None
        while time.time() < deadline:
            undone = await self.list_undone_copy()
            done = await self.list_done_copy()
            for t in undone + done:
                if t.id == task_id:
                    last = t
                    if t.state in FINISHED_STATES:
                        return t
                    break
            await asyncio.sleep(poll_interval)
        if last is None:
            raise TimeoutError(f"任务 {task_id} 在 AList 中未找到")
        raise TimeoutError(
            f"任务 {task_id} 超时未完成 (state={last.state}, progress={last.progress:.1f})"
        )
