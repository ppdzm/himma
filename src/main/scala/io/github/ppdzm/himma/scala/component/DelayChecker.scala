package io.github.ppdzm.himma.scala.component

import io.github.ppdzm.utils.universal.base.Logging

import java.util.Calendar
import java.util.concurrent.Executors

/**
 * Created by Stuart Alex on 2017/7/25.
 */
trait DelayChecker {
    protected val logging = new Logging(getClass)
    private val threadPool = Executors.newFixedThreadPool(1)
    private var currentProgress = 0L

    /**
     * 启动延迟检查器
     *
     * @param initialValue     进度初始值
     * @param interval         延迟检查时间间隔（毫秒）
     * @param threshold        允许的延迟最大阈值（毫秒）
     * @param delayHandler     检查到延迟时的处理方法
     * @param heartbeat        心跳方法
     * @param checkImmediately 是否在启动之初立即进行检查
     */
    def startDelayChecker(initialValue: Long, interval: Int, threshold: Int, delayHandler: (Long, Int) => Unit, heartbeat: () => Unit, checkImmediately: Boolean = true): Unit = {
        this.threadPool.execute(DelayChecker(initialValue, interval, threshold, delayHandler, heartbeat, checkImmediately))
        this.logging.logInfo("DelayChecker started")
    }

    /**
     * 更新进度
     *
     * @param value 新的进度值
     */
    def setProgress(value: Long): Unit = this.currentProgress = value

    /**
     * 当前时间戳与作业进度
     *
     * @param initialValue     进度初始值
     * @param interval         延迟检查时间间隔（毫秒）
     * @param threshold        允许的延迟最大阈值（毫秒）
     * @param delayHandler     检查到延迟时的处理方法
     * @param heartbeat        心跳方法
     * @param checkImmediately 是否在启动之初立即进行检查
     */
    case class DelayChecker(initialValue: Long, interval: Int, threshold: Int, delayHandler: (Long, Int) => Unit, heartbeat: () => Unit, checkImmediately: Boolean) extends Runnable {

        override def run(): Unit = {
            currentProgress = initialValue
            Thread.sleep(5000)
            if (!checkImmediately)
                Thread.sleep(interval)
            val nowCalendar = Calendar.getInstance()
            while (true) {
                nowCalendar.setTimeInMillis(System.currentTimeMillis())
                val hour = nowCalendar.get(Calendar.HOUR_OF_DAY)
                val delay = nowCalendar.getTimeInMillis - currentProgress
                if (delay >= this.threshold) {
                    delayHandler(delay, hour)
                }
                heartbeat()
                Thread.sleep(interval)
            }
            threadPool.shutdown()
        }

    }

}
