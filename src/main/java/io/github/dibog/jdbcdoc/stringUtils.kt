package io.github.dibog.jdbcdoc

import kotlin.math.max


fun String.rpad(padLength: Int, padding: String = " "): String {
    val diff = padLength - length

    val truncating = if(diff<=0) { // string is longer then space
        this
    }
    else {
        val paddingLength = padding.length
        val multiple = diff / paddingLength
        this+padding.repeat(multiple)
    }

    return truncating.substring(0, padLength)
}

fun String.lpad(padLength: Int, padding: String = " "): String {
    val diff = padLength - length

    val truncating = if(diff<=0) { // string is longer then space
        this
    }
    else {
        val paddingLength = padding.length
        val multiple = diff / paddingLength
        padding.repeat(multiple)+this
    }

    val truncLen = truncating.length
    return truncating.substring(truncLen-padLength, truncLen)
}

fun String.cpad(padLength: Int, lpad: String = " ", rpad: String = " "): String {
    val paddingHalf = padLength / 2
    val half = length / 2

    return if(half<=0) {
        ("".lpad(paddingHalf, lpad)) + ("".rpad(padLength-paddingHalf, rpad))
    } else {
        val leftHalf = substring(0, half)
        val rightHalf = substring(half, length)

        val padHalf = padLength / 2
        val padOtherHalf = padLength - padHalf

        leftHalf.lpad(padHalf, lpad) + rightHalf.rpad(padOtherHalf, rpad)
    }
}

enum class ColumnAlignment { LEFT, CENTER, RIGHT;

    fun pad(text: String, padLength: Int, padding: String = " "): String {
        return when(this) {
            LEFT -> text.rpad(padLength, padding)
            CENTER -> text.cpad(padLength, padding, padding)
            RIGHT -> text.lpad(padLength, padding)
        }
    }
}

fun <R> List<R>.toTableString(
        alignments : List<ColumnAlignment> = listOf(ColumnAlignment.LEFT),
        headers: List<String>? = null,
        noContent: String = "No content",
        transform: (R)->List<String>
) : String {
    if(isEmpty()) return "$noContent\n"

    val sb = StringBuilder()

    fun countColumnWidths(): IntArray {
        val line = transform(this[0])

        val maxArray = if(headers==null) {
            IntArray(line.size) { Integer.MIN_VALUE }
        }
        else {
            IntArray(line.size) { headers[it]?.length ?: 0 }
        }


        forEach { line ->
            val line = transform(line)
            line.forEachIndexed { index, text ->
                maxArray[index] = max(maxArray[index], (text ?: "").length)
            }
        }

        return maxArray
    }

    fun appendLine(line: List<String>, columnWidths: IntArray) {
        sb.append("|")
        var padding = ColumnAlignment.LEFT
        line.forEachIndexed { columnIndex, column ->
            if (alignments.size > columnIndex) {
                padding = alignments[columnIndex]
            }
            sb.append(" ${padding.pad(column ?: "", columnWidths[columnIndex])} |")
        }
        sb.append("\n")
    }

    val columnWidths = countColumnWidths()
    val lineLength = columnWidths.sumBy { it } + 4 + (columnWidths.size-1)*3

    if(headers!=null) {
        appendLine(headers, columnWidths)
        sb.append("=".repeat(lineLength))
        sb.append("\n")
    }

    forEach{ line ->
        appendLine(transform(line), columnWidths)
    }

    return sb.toString()
}

fun String.println() = println(this)