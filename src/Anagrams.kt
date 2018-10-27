package com.georgegarside.coc105.anagrams

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.FileSystem
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
import java.util.*
import kotlin.system.exitProcess

class Anagrams : Configured(), Tool {
	class AnagramMapper : Mapper<LongWritable, Text, Text, SetWritable>() {
		private val keyOut = Text()
		private val valOut = SetWritable()
		override fun map(key: LongWritable, value: Text, context: Context) = value
				.toString()
				.split("[^\\w'’-]|--|_".toRegex())
				.asSequence()
				.filterNot { it.length == 1 || it.matches(".*\\d.*|(.)\\1+".toRegex()) }
				.map { it.toLowerCase(Locale.getDefault()) }
				.forEach { word ->
					val chars = word.toCharArray().sortedArray().filterNot { c -> c == '-' || c == '\'' || c == '’' }.joinToString(separator = "")
					context.write(keyOut.apply { set(chars) }, valOut.apply { values = setOf(word) })
				}
	}

	class AnagramReducer : Reducer<Text, SetWritable, Text, SetWritable>() {
		override fun reduce(key: Text, values: MutableIterable<SetWritable>, context: Context) {
			context.write(key, values.reduce { acc, cur ->
				val vals = (acc.values + cur.values)
				SetWritable(vals)
			})
		}
	}

	class SetWritable(var values: Set<String>) : Writable {
		constructor() : this(setOf())

		override fun readFields(input: DataInput) {
			val size = input.readInt()
			values = (0 until size).fold(setOf()) { acc, _ ->
				acc + input.readUTF()
			}
		}

		override fun write(output: DataOutput) {
			output.writeInt(values.size)
			values.forEach { output.writeUTF(it) }
		}

		operator fun plus(other: SetWritable): SetWritable {
			values += other.values
			return this
		}

		override fun toString() = values.joinToString(prefix = "{ ", separator = ", ", postfix = " }")
	}

	override fun run(vararg args: String) =
			try {
				run(Path(args[0]), Path(args[1]))
			} catch (e: Exception) {
				when (e) {
					is IllegalArgumentException, is ArrayIndexOutOfBoundsException -> {
						ToolRunner.printGenericCommandUsage(System.err)
						RuntimeException("Usage: ${this::class.simpleName} <input> <output>")
						-1
					}
					else -> throw e
				}
			}

	private fun run(input: Path, output: Path): Int =
			Job.getInstance(conf, this::class.simpleName)
					.apply {
						setJarByClass(this@Anagrams::class.java)

						mapperClass = Anagrams.AnagramMapper::class.java
						reducerClass = Anagrams.AnagramReducer::class.java
						numReduceTasks = 1

						inputFormatClass = TextInputFormat::class.java

						outputKeyClass = Text::class.java
						outputValueClass = SetWritable::class.java

						outputFormatClass = AnagramOutput::class.java
					}
					.also {
						FileInputFormat.addInputPath(it, input)
						FileOutputFormat.setOutputPath(it, output
								.also { path ->
									FileSystem.get(conf)
											.apply {
												if (exists(path)) delete(path, true)
											}
								})
					}
					.let {
						if (it.waitForCompletion(true)) 0 else 1
					}

	companion object {
		class AnagramOutput<K, V> : TextOutputFormat<K, V>() {
			override fun getRecordWriter(job: TaskAttemptContext): RecordWriter<K, V> =
					AnagramLineOutput(getDefaultWorkFile(job, ".txt").let {
						it.getFileSystem(job.configuration).create(it, true)
					})

			private inner class AnagramLineOutput(out: DataOutputStream) : LineRecordWriter<K, V>(out, "") {
				override fun write(key: K, value: V) {
					if (value is SetWritable && value.values.size == 1) return
					super.write(null, value)
				}
			}
		}
	}
}

fun main(vararg args: String): Unit = exitProcess(ToolRunner.run(Anagrams(), args))
