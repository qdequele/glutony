#!/usr/bin/env python3
"""Checks that the fake Tinybird aggregates like the real pipe.

Run with: python3 -I scripts/test_fake_tinybird.py
"""

import importlib.util
import pathlib
import unittest

_SPEC = importlib.util.spec_from_file_location(
    "fake_tinybird", pathlib.Path(__file__).with_name("fake_tinybird.py")
)
fake = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fake)


def job(n: int, status: str, documents_out: int = 0) -> dict:
    return {
        "event_id": f"job-{n}:job",
        "kind": "job",
        "ts": "2026-11-02T10:00:00.000Z",
        "project_id": "acme",
        "pipeline_uid": "builtin.pdf",
        "plugin": "",
        "status": status,
        "documents_out": documents_out,
    }


def step(n: int, documents_out: int) -> dict:
    return {
        "event_id": f"job-{n}:extract:1",
        "kind": "step",
        "ts": "2026-11-02T10:00:00.000Z",
        "project_id": "acme",
        "pipeline_uid": "builtin.pdf",
        "plugin": "pdf_extractor",
        "status": "succeeded",
        "documents_out": documents_out,
        "pages": 2,
    }


class AggregateTest(unittest.TestCase):
    def setUp(self) -> None:
        fake.ROWS.clear()

    def test_job_counters_follow_the_job_status(self) -> None:
        for n, status in enumerate(["succeeded", "succeeded", "succeeded", "failed", "cancelled"]):
            row = job(n, status, documents_out=4)
            fake.ROWS[row["event_id"]] = row
        rows = fake.aggregate("acme", "2026-11-02", "2026-11-02")
        self.assertEqual(len(rows), 1)
        jobs = rows[0]
        self.assertEqual(jobs["plugin"], "")
        self.assertEqual(jobs["jobs"], 5)
        self.assertEqual(jobs["jobs_succeeded"], 3)
        self.assertEqual(jobs["jobs_failed"], 1, "a cancelled job is neither")
        self.assertEqual(jobs["documents_indexed"], 20)
        self.assertEqual(jobs["steps"], 0)

    def test_step_counters_come_from_step_rows(self) -> None:
        for n in range(2):
            row = step(n, documents_out=3)
            fake.ROWS[row["event_id"]] = row
        rows = fake.aggregate("acme", "2026-11-02", "2026-11-02")
        self.assertEqual(len(rows), 1)
        steps = rows[0]
        self.assertEqual(steps["plugin"], "pdf_extractor")
        self.assertEqual(steps["steps"], 2)
        self.assertEqual(steps["documents_out"], 6)
        self.assertEqual(steps["pages"], 4)
        self.assertEqual(steps["jobs"], 0)


if __name__ == "__main__":
    unittest.main()
