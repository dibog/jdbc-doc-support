package io.github.dibog.jdbcdoc

fun <In,Out,NewKey> Collection<In>.collectWith(collect: (In)->Pair<NewKey,Out>): Map<NewKey,List<Out>> {
    val result = mutableMapOf<NewKey,MutableList<Out>>()
    this.forEach { inElem ->
        val (key,out) = collect(inElem)
        val f = result.getOrPut(key, { mutableListOf<Out>()}).add(out)
    }
    return result
}

fun <In,NewKey> Collection<In>.collectBy(collect: (In)->NewKey): Map<NewKey,List<In>> {
    val result = mutableMapOf<NewKey,MutableList<In>>()
    this.forEach { inElem ->
        val key = collect(inElem)
        val f = result.getOrPut(key, { mutableListOf<In>()}).add(inElem)
    }
    return result
}