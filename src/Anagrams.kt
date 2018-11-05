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

class Anagrams : Configured(), Tool {
	class AnagramMapper : Mapper<LongWritable, Text, Text, SetWritable>() {
		private val keyOut = Text()
		private val valOut = SetWritable()
		override fun map(key: LongWritable, value: Text, context: Context) = value
				.toString()
				.split(" |--|_".toRegex())
				.asSequence()
				.map {
					"""[\pL\p{Mn}][\pL\p{Mn}$wordJoiners]+[\pL\p{Mn}]""".toRegex(RegexOption.IGNORE_CASE)
							.find(it)?.value
							?: ""
				}
				.filterNot { it.length <= 1 || it.matches(".*\\d.*|(.)\\1+".toRegex()) }
				.forEach { word ->

					keyOut.set(word.toLowerCase().toCharArray().sortedArray().filterNot { c -> wordJoiners.toCharArray().any { it == c } }.joinToString(separator = ""))

					valOut.value =
							if (word.toCharArray().asSequence().filter { it.isUpperCase() }.toList().size > 1) {
								setOf(word.toLowerCase())
							} else {
								setOf(word)
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
			context.write(key, values.reduce { acc, cur ->
				cur.value.fold(acc.value) { set, s ->

					// { Abc }, abc (lowercase of existing word) -> { abc } (remove Abc and add abc)
					set.firstOrNull { it.toLowerCase() == s }?.let { set - it + s } ?:

					// { abc }, Abc (uppercase of existing word) -> { abc } (leave set as existing)
					set.firstOrNull { it == s.toLowerCase() }?.let { set } ?:

					// { abc }, cab (word does not exist) -> { abc, cab } (merge sets)
					set+s

				}.let {
					SetWritable(it)
				}
			})
		}
	}

	class SetWritable(var value: Set<String>) : Writable {
		constructor() : this(setOf())

		override fun readFields(input: DataInput) {
			val size = input.readInt()
			value = (0 until size).fold(setOf()) { acc, _ ->
				acc + input.readUTF()
			}
		}

		override fun write(output: DataOutput) {
			output.writeInt(value.size)
			value.forEach { output.writeUTF(it) }
		}

		override fun toString() = value.joinToString(prefix = "{ ", separator = ", ", postfix = " }")
	}

	override fun run(vararg args: String) = run(Path(args[0]), Path(args[1]))

	private fun run(input: Path, output: Path): Int =
			Job.getInstance(conf, this::class.simpleName)
					.apply {
						setJarByClass(this@Anagrams::class.java)

						mapperClass = Anagrams.AnagramMapper::class.java
						partitionerClass = Anagrams.AnagramPartitioner::class.java
						combinerClass = Anagrams.AnagramReducer::class.java
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
					.let {
						if (it.waitForCompletion(true)) 0 else 1
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
