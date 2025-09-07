package io.obscura.laplace.tap

import org.agrona.concurrent.UnsafeBuffer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.min

class SpscByteRing(capacityBytes: Int, private val maxMsg: Int) {
    private val cap = nextPow2(capacityBytes)
    private val buf = UnsafeBuffer(ByteBuffer.allocateDirect(cap).order(ByteOrder.BIG_ENDIAN))

    private val maskL: Long = (cap - 1).toLong()
    private val maskI: Int = cap - 1
    private val head = AtomicLong(0)
    private val tail = AtomicLong(0)

    init {
        require(maxMsg <= cap - 4) { "maxMsg must fit with 4-byte header" }
    }

    fun offer(src: ByteArray, off: Int = 0, lenIn: Int = src.size): Boolean {
        require(off >= 0 && lenIn >= 0 && off + lenIn <= src.size) {
            "Invalid (off,lenIn) for src"
        }

        val len = min(lenIn, maxMsg)
        val need = 4 + len

        val t = tail.get()
        val h = head.get()

        //drop if not enough cap
        if (need > cap || t - h + need > cap) return false

        val base = wrap(t)

        writeHeader(base, len)
        writePayload(base, len, src, off)

        tail.lazySet(t + need)
        return true
    }


    fun poll(limit: Int, consumer: (ByteArray) -> Unit): Int {
        var polled = 0
        var h = head.get()
        val t = tail.get()

        while (polled < limit && h < t) {
            val base = (h and maskL).toInt()

            val len = readHeader(base)

            val need = 4 + len
            val out = readPayload(len, base)

            head.lazySet(h + need)
            consumer(out)
            polled++
            h += need
        }
        return polled
    }

    fun approxFree(): Int = cap - (tail.get() - head.get()).toInt()

    private fun nextPow2(x: Int): Int {
        var v = if (x < 2) 2 else x - 1
        v = v or (v shr 1)
        v = v or (v shr 2)
        v = v or (v shr 4)
        v = v or (v shr 8)
        v = v or (v shr 16)
        return v + 1
    }

    private inline fun wrap(counter: Long): Int =
        ((counter and maskL).toInt())

    private inline fun wrap(index: Int): Int =
        (index and maskI)

    private fun writeHeader(base: Int, len: Int) {
        if (base <= cap - 4) {
            buf.putInt(base, len)
        } else {
            buf.putByte(wrap(base), ((len ushr 24) and 0xFF).toByte())
            buf.putByte(wrap(base + 1), ((len ushr 16) and 0xFF).toByte())
            buf.putByte(wrap(base + 2), ((len ushr 8) and 0xFF).toByte())
            buf.putByte(wrap(base + 3), (len and 0xFF).toByte())
        }
    }

    private fun writePayload(base: Int, len: Int, src: ByteArray, off: Int) {
        var wrote = 0
        var dst = wrap(base + 4)
        while (wrote < len) {
            val chunk = min(len - wrote, cap - dst)
            buf.putBytes(dst, src, off + wrote, chunk)
            dst = wrap(dst + chunk)
            wrote += chunk
        }
    }

    private fun readHeader(base: Int): Int {
        return if (base <= cap - 4) {
            buf.getInt(base)
        } else {
            ((buf.getByte(wrap(base)).toInt() and 0xFF) shl 24) or
                    ((buf.getByte(wrap(base + 1)).toInt() and 0xFF) shl 16) or
                    ((buf.getByte(wrap(base + 2)).toInt() and 0xFF) shl 8) or
                    (buf.getByte(wrap(base + 3)).toInt() and 0xFF)
        }
    }

    private fun readPayload(len: Int, base: Int): ByteArray {
        val out = ByteArray(len)
        var read = 0
        var src = wrap(base + 4)
        while (read < len) {
            val chunk = min(len - read, cap - src)laz
            buf.getBytes(src, out, read, chunk)
            src = wrap(src + chunk)
            read += chunk
        }

        return out;
    }
}