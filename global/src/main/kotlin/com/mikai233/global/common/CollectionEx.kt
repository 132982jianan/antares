package com.mikai233.global.common

fun Collection<Long>.formatIds(limit: Int = 20): String {
    if (isEmpty()) {
        return "[]"
    }
    val values = take(limit).joinToString(prefix = "[", postfix = "]")
    return if (size <= limit) {
        values
    } else {
        values.dropLast(1) + ", ... +${size - limit}]"
    }
}
