package io.github.ppdzm.himma.scala.recorder

/**
 * 记录快照信号Ready状态
 */
trait SignalRecorder {

    def initialize()

    def set(signal: String)

    def reset(signal: String)

    def elect(signal: String): Boolean

    def isReady(signal: String): Boolean

}
