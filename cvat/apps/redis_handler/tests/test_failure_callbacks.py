# Copyright (C) CVAT.ai Corporation
#
# SPDX-License-Identifier: MIT

import unittest
from unittest import mock

from django.conf import settings
from django.test import override_settings
from fakeredis import FakeRedis
from rest_framework.exceptions import NotFound
from rq import Queue, Retry, SimpleWorker
from rq.job import Job

from cvat.apps.engine.cache import (
    Callback,
    ChunkCreationError,
    enqueue_create_chunk_job,
    wait_for_rq_job,
)
from cvat.apps.engine.rq import BaseRQMeta, RQJobMetaField, save_job_failure_metadata
from cvat.apps.engine.views import rq_exception_handler
from cvat.apps.redis_handler.serializers import RequestSerializer
from cvat.apps.redis_handler.utils import send_request_failed_signal


def missing_cloud_image():
    raise NotFound("Cloud image is missing")


class TestChunkFailureMetadata(unittest.TestCase):
    def test_failure_is_available_before_global_exception_handler(self):
        connection = FakeRedis()
        self.addCleanup(connection.close)
        queue = Queue(settings.CVAT_QUEUES.CHUNKS.value, connection=connection)
        job = enqueue_create_chunk_job(queue, "test-chunk", Callback(callable=missing_cloud_image))
        worker = SimpleWorker([queue], connection=connection, prepare_for_work=False)

        def read_failed_job(*args, **kwargs):
            # RQ has published FAILED, but has not run the global exception handlers yet.
            reader_job = Job.fetch(job.id, connection=connection)
            self.assertTrue(reader_job.is_failed)
            with self.assertRaises(NotFound) as caught:
                wait_for_rq_job(reader_job)
            self.assertEqual(str(caught.exception.detail), "Cloud image is missing")

        with mock.patch.object(worker, "handle_exception", side_effect=read_failed_job) as handler:
            self.assertFalse(worker.perform_job(job, queue))
        handler.assert_called_once()

    @override_settings(CVAT_CHUNK_FAILURE_META_TIMEOUT=0.4, CVAT_CHUNK_CREATE_CHECK_INTERVAL=0.2)
    def test_failure_without_metadata_uses_fallback(self):
        job = mock.Mock(meta={}, id="job-without-meta", exc_info=None)
        job.get_status.return_value = "failed"
        with (
            mock.patch("cvat.apps.engine.cache.time.sleep") as sleep,
            self.assertRaisesRegex(ChunkCreationError, "Cannot create chunk"),
        ):
            wait_for_rq_job(job)
        # Wait briefly for metadata that never arrives, then fall back.
        self.assertGreaterEqual(sleep.call_count, 1)
        job.refresh.assert_called()

    @override_settings(CVAT_CHUNK_FAILURE_META_TIMEOUT=0.4, CVAT_CHUNK_CREATE_CHECK_INTERVAL=0.2)
    def test_failure_waits_for_metadata_appearing_after_failed_status(self):
        # Regression for https://github.com/cvat-ai/cvat/issues/11147:
        # FAILED can become visible one poll before exc_type/exc_args are saved.
        job = mock.Mock(id="job-race", exc_info=None)
        job.get_status.return_value = "failed"
        job.meta = {}
        refresh_calls = {"n": 0}

        def refresh_side_effect():
            refresh_calls["n"] += 1
            if refresh_calls["n"] >= 2:
                job.meta = {
                    RQJobMetaField.EXCEPTION_TYPE: NotFound,
                    RQJobMetaField.EXCEPTION_ARGS: ("Cloud image is missing",),
                }

        job.refresh.side_effect = refresh_side_effect

        with mock.patch("cvat.apps.engine.cache.time.sleep"):
            with self.assertRaises(NotFound) as caught:
                wait_for_rq_job(job)

        self.assertEqual(str(caught.exception.detail), "Cloud image is missing")
        self.assertGreaterEqual(job.refresh.call_count, 2)


def failing_request():
    try:
        raise RuntimeError("Internal cause")
    except RuntimeError as cause:
        raise ValueError("Request failed") from cause


class TestRequestFailureMetadata(unittest.TestCase):
    def setUp(self):
        self.connection = FakeRedis()
        self.addCleanup(self.connection.close)
        self.queue = Queue(settings.CVAT_QUEUES.IMPORT_DATA.value, connection=self.connection)
        self.worker = SimpleWorker([self.queue], connection=self.connection, prepare_for_work=False)

    def test_message_is_available_before_global_exception_handler(self):
        job = self.queue.enqueue(failing_request, on_failure=send_request_failed_signal)

        def read_failed_job(*args, **kwargs):
            reader_job = Job.fetch(job.id, connection=self.connection)
            self.assertTrue(reader_job.is_failed)
            serializer = RequestSerializer()
            serializer._base_rq_job_meta = BaseRQMeta.for_job(reader_job)
            self.assertEqual(serializer.get_message(reader_job), "ValueError: Request failed\n")

        with mock.patch.object(
            self.worker, "handle_exception", side_effect=read_failed_job
        ) as handler:
            self.assertFalse(self.worker.perform_job(job, self.queue))
        handler.assert_called_once()

    def test_retry_does_not_save_terminal_failure_or_send_signal(self):
        job = self.queue.enqueue(
            failing_request, on_failure=send_request_failed_signal, retry=Retry(max=1)
        )
        with (
            mock.patch(
                "cvat.apps.redis_handler.utils.signals.request_failed.send_robust"
            ) as signal,
            mock.patch.object(self.worker, "handle_exception"),
        ):
            self.assertFalse(self.worker.perform_job(job, self.queue))
        reader_job = Job.fetch(job.id, connection=self.connection)
        self.assertTrue(reader_job.is_queued)
        self.assertIsNone(BaseRQMeta.for_job(reader_job).formatted_exception)
        signal.assert_not_called()

    def test_global_handler_preserves_existing_metadata_and_fills_missing_metadata(self):
        for already_saved in (False, True):
            with self.subTest(already_saved=already_saved):
                job = self.queue.enqueue(failing_request)
                job.set_status("failed")
                error = ValueError("Request failed")
                if already_saved:
                    save_job_failure_metadata(job, type(error), error)
                with mock.patch(
                    "cvat.apps.engine.views.save_job_failure_metadata",
                    wraps=save_job_failure_metadata,
                ) as save:
                    rq_exception_handler(job, type(error), error, None)
                self.assertEqual(save.call_count, 0 if already_saved else 1)
                reader_job = Job.fetch(job.id, connection=self.connection)
                self.assertEqual(
                    BaseRQMeta.for_job(reader_job).formatted_exception,
                    "ValueError: Request failed\n",
                )
