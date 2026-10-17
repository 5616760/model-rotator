#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""端点多路复用回归测试。

背景：WorkBuddy 的部分 agent/工具走 OpenAI Responses API（POST /v1/responses），
代理原先只放行 /v1/chat/completions，导致这类请求被自己的 404 挡掉：
  unexpected status 404 Not Found: unsupported endpoint: /v1/responses

要点：
- 白名单要包含 /v1/responses，且不能因为 query string 或尾斜杠被误判；
- 上游 URL 必须按端点在同一个 base 下重建 —— chat/completions 是两级路径，
  只替换末段会拼出 /v1/chat/responses（本次实际踩到的 bug）；
- 未知端点仍然要 404，别把整站变成开放代理。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import rotator  # noqa: E402


def test_upstream_url_rewrites_endpoint_under_same_base():
    base = "http://127.0.0.1:3065/v1/chat/completions"
    assert rotator.upstream_url(base, "/v1/responses") == \
        "http://127.0.0.1:3065/v1/responses"
    assert rotator.upstream_url(base, "/v1/chat/completions") == base
    print("OK endpoint rewrite  ->", rotator.upstream_url(base, "/v1/responses"))


def test_upstream_url_keeps_path_prefix():
    # 上游可能挂在 /openai 之类的子路径下，前缀必须保留
    base = "https://api.example.com/openai/v1/chat/completions"
    got = rotator.upstream_url(base, "/v1/responses")
    assert got == "https://api.example.com/openai/v1/responses", got
    print("OK keeps prefix      ->", got)


def test_upstream_url_does_not_produce_nested_chat():
    # 回归：曾拼出 /v1/chat/responses
    base = "http://h/v1/chat/completions"
    got = rotator.upstream_url(base, "/v1/responses")
    assert "/v1/chat/responses" not in got, got
    assert got == "http://h/v1/responses", got
    print("OK no nested chat    ->", got)


def test_supported_paths_include_responses():
    assert "/v1/chat/completions" in rotator.SUPPORTED_POST_PATHS
    assert "/v1/responses" in rotator.SUPPORTED_POST_PATHS
    print("OK whitelist         ->", rotator.SUPPORTED_POST_PATHS)


def test_path_normalization():
    """模拟 do_POST 的归一化：query string 与尾斜杠不应影响判定。"""
    def norm(p):
        return p.split("?", 1)[0].rstrip("/")

    assert norm("/v1/responses") in rotator.SUPPORTED_POST_PATHS
    assert norm("/v1/responses/") in rotator.SUPPORTED_POST_PATHS
    assert norm("/v1/responses?api-version=2025-04-01") in rotator.SUPPORTED_POST_PATHS
    assert norm("/v1/chat/completions") in rotator.SUPPORTED_POST_PATHS
    # 未知端点必须仍然被拒
    assert norm("/v1/embeddings") not in rotator.SUPPORTED_POST_PATHS
    assert norm("/v1/images/generations") not in rotator.SUPPORTED_POST_PATHS
    print("OK path normalization (unknown still rejected)")


if __name__ == "__main__":
    test_supported_paths_include_responses()
    test_upstream_url_rewrites_endpoint_under_same_base()
    test_upstream_url_keeps_path_prefix()
    test_upstream_url_does_not_produce_nested_chat()
    test_path_normalization()
    print("\nALL ENDPOINT ROUTING TESTS PASSED")
