package io.rzeszut.flyingducks

import org.apache.arrow.flight.CallStatus
import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.flight.FlightProducer.StreamListener
import org.slf4j.LoggerFactory

object ErrorHandler {

  private val log = LoggerFactory.getLogger(ErrorHandler::class.java)

  fun <T> wrap(block: () -> T): T {
    try {
      return block.invoke()
    } catch (e: Throwable) {
      log.error("Unexpected error occurred", e)
      throw convertError(e)
    }
  }

  fun wrap(listener: ServerStreamListener, block: () -> Unit) {
    try {
      block.invoke()
    } catch (e: Throwable) {
      log.error("Unexpected error occurred", e)
      listener.error(convertError(e))
    }
  }

  fun wrap(listener: StreamListener<out Any>, block: () -> Unit) {
    try {
      block.invoke()
    } catch (e: Throwable) {
      log.error("Unexpected error occurred", e)
      listener.onError(convertError(e))
    }
  }

  private fun convertError(e: Throwable) =
    CallStatus.INTERNAL
      .withCause(e)
      .withDescription(e.message)
      .toRuntimeException()
}