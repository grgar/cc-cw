package com.georgegarside.coc105.anagrams

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import java.io.DataInput
import java.io.DataOutput
import java.io.DataOutputStream
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis

class Anagrams : Configured(), Tool {
	class AnagramMapper : Mapper<LongWritable, Text, Text, SetWritable>() {
		private var keyOut = Text()
		private var valOut = SetWritable()

		var Text.value: String
			get() = toString()
			set(value) = set(value)

		public override fun map(key: LongWritable, value: Text, context: Context) = value
				.toString()
				.split(" |--|_".toRegex())
				.asSequence()
				.map {
					"""[\pL\p{Mn}][\pL\p{Mn}$wordJoiners]*[\pL\p{Mn}]"""
							.toRegex(RegexOption.IGNORE_CASE)
							.find(it)
							?.value
							?: ""
				}
				.filterNot { it.length <= 1 || it.matches(""".*\d.*|(.)\1+""".toRegex()) }
				.forEach { word ->

					keyOut.value = word
							.toLowerCase()
							.filterNot { c -> wordJoiners.any { joiner -> c == joiner } }
							.toCharArray()
							.sortedArray()
							.joinToString(separator = "")

					word
							.toCharArray()
							.asSequence()
							.filter { it.isUpperCase() }
							.toList()
							.let {
								valOut.value =
										if (it.size > 1) {
											setOf(word.toLowerCase())
										} else {
											setOf(word)
										}
							}

					context.write(keyOut, valOut)
				}
	}

	class AnagramPartitioner : Partitioner<Text, SetWritable>() {
		override fun getPartition(text: Text, set: SetWritable, partitions: Int): Int {
			return text.length % partitions
		}
	}

	class AnagramReducer : Reducer<Text, SetWritable, Text, SetWritable>() {
		override fun reduce(key: Text, values: MutableIterable<SetWritable>, context: Context) {
			context.write(key, SetWritable(values.fold(setOf()) { acc, setWritable ->
				acc anagramWith setWritable.value
			}))
		}

		infix fun Set<String>.anagramWith(right: Set<String>) = anagram(this, right)

		data class Stats(val upper: Int, val joiner: Int) : Comparable<Stats> {
			/**
			 * Compares this object with the specified object for order. Returns zero if this object is equal
			 * to the specified [other] object, a negative number if it's less than [other], or a positive number
			 * if it's greater than [other].
			 */
			override fun compareTo(other: Stats) =
					joiner.compareTo(other.joiner).let { if (it != 0) it else upper.compareTo(other.upper) }
		}

		fun String.stats() = Stats(
				upper = count { char -> char.isUpperCase() },
				joiner = count { char -> wordJoiners.toCharArray().any { joinerChar -> char == joinerChar } }
		)

		val joinedPositions = { word: String ->
			"""[$wordJoiners]""".toRegex()
					.findAll(word)
					.fold(setOf<Pair<Int, Char>>()) { acc, matchResult ->
						acc + (matchResult.range.first to matchResult.value.first())
					}
		}

		fun anagram(left: Set<String>, right: Set<String>) = right.fold(left) { set, s ->
			// { Abc }, abc (lowercase of existing word) -> { abc } (remove Abc and add abc)
			set.find { it.toLowerCase() == s || it.replace("""[$wordJoiners]""".toRegex(), "") == s }
					?.let { set - it + s } ?:

			// { Foo }, Fo'o (apostrophe of existing word) -> { Foo } (leave set as existing)
			set.find { it == s.replace("""[$wordJoiners]""".toRegex(), "") }
					?.let { set } ?:

			// { Fo'o }, foo (lowercase and no apostrophe of existing word) -> { foo } (remove Fo'o and add foo)
			set.find { it.replace("""[$wordJoiners]""".toRegex(), "").toLowerCase() == s.toLowerCase() }
					?.let { set - it + s.toLowerCase() } ?:

			// { foo }, Fo'o (uppercase and apostrophe of existing word) -> { foo } (leave set as existing)
			set.find { it.toLowerCase() == s.replace("""[$wordJoiners]""".toRegex(), "").toLowerCase() }
					?.let { set - it + s.replace("""[$wordJoiners]""".toRegex(), "").toLowerCase() } ?:

			// { abc }, Abc (uppercase of existing word) -> { abc } (leave set as existing)
			set.firstOrNull { it == s.toLowerCase() || it == s.replace("""[$wordJoiners]""".toRegex(), "") }
					?.let { set } ?:

			// { fo'o }, f-oo (same word, differing with word joiners) -> {remove all word joiners and return original word}
			// { abc }, abc (word already exists as is) -> { abc } (leave set as existing)
			set.firstOrNull { it.replace("""[$wordJoiners]""".toRegex(), "").toLowerCase() == s.replace("""[$wordJoiners]""".toRegex(), "").toLowerCase() }
					?.let {
						if (joinedPositions(it) == joinedPositions(s)) {
							set
						} else {
							set - it + s.replace("""[$wordJoiners]""".toRegex(), "")
						}
					} ?:

			// { abc }, cab (word does not exist) -> { abc, cab } (merge sets)
			set+s
		}
	}

	class SetWritable(var value: Set<String>) : Writable {
		constructor() : this(setOf())

		override fun readFields(input: DataInput): Unit =
				(0 until input.readInt())
						.fold<Int, Set<String>>(setOf()) { acc, _ ->
							acc + input.readUTF()
						}
						.let { value = it }

		override fun write(output: DataOutput) =
				value
						.also { output.writeInt(it.size) }
						.forEach { output.writeUTF(it) }

		override fun toString() =
				value.joinToString(prefix = "{ ", separator = ", ", postfix = " }")
	}

	override fun run(vararg args: String) = run(Path(args[0]), Path(args[1]))

	private fun run(input: Path, output: Path): Int =
			Job.getInstance(conf, this::class.simpleName)
					.apply {
						setJarByClass(this@Anagrams::class.java)

						mapperClass = Anagrams.AnagramMapper::class.java
						partitionerClass = Anagrams.AnagramPartitioner::class.java
						reducerClass = Anagrams.AnagramReducer::class.java

						inputFormatClass = TextInputFormat::class.java

						outputKeyClass = Text::class.java
						outputValueClass = SetWritable::class.java

						outputFormatClass = AnagramOutput::class.java
					}
					.also {
						FileInputFormat.addInputPath(it, input)
						FileOutputFormat.setOutputPath(it, output)
					}
					.let { job ->
						println(measureTimeMillis { job.waitForCompletion(true) })
						0
					}

	companion object {
		const val wordJoiners = "'’-"

		class AnagramOutput<K, V> : TextOutputFormat<K, V>() {
			override fun getRecordWriter(job: TaskAttemptContext): RecordWriter<K, V> =
					AnagramLineOutput(getDefaultWorkFile(job, ".txt").let {
						it.getFileSystem(job.configuration).create(it, true)
					})

			private inner class AnagramLineOutput(out: DataOutputStream) : LineRecordWriter<K, V>(out, "") {
				override fun write(key: K, value: V) {
					if (value is SetWritable && value.value.size == 1) return
					super.write(null, value)
				}
			}
		}
	}
}

fun main(vararg args: String): Unit = exitProcess(ToolRunner.run(Anagrams(), args))
