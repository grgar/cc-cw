import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import java.io.DataOutputStream
import java.util.*
import kotlin.system.exitProcess

class Anagrams : Configured(), Tool {
	class AnagramMapper : Mapper<LongWritable, Text, Text, TextArrayWritable>() {
		private val keyOut = Text()
		private val valOut = TextArrayWritable()
		private val valString = Text()
		override fun map(key: LongWritable, value: Text, context: Context) = value
				.toString()
				.split("[^\\w]".toRegex())
				.asSequence()
				.filterNot { it.length == 1 || it.matches(".*\\d.*".toRegex()) }
				.map { it.toLowerCase(Locale.getDefault()).trim('_') }
				.filter { it.matches("\\w+".toRegex()) }
				.filterNot { it.matches("(.)\\1+".toRegex()) }
				.forEach {
					val chars = it.toCharArray().sortedArray().joinToString(separator = "")
					context.write(keyOut.apply { set(chars) }, TextArrayWritable().apply { set(arrayOf(Text(it))) })
				}
	}

	class AnagramReducer : Reducer<Text, TextArrayWritable, Text, TextArrayWritable>() {
		override fun reduce(key: Text, values: MutableIterable<TextArrayWritable>, context: Context) {
			context.write(key, values.reduce { acc, cur ->
				val list = (acc.toStrings() + cur.toStrings()).toSortedSet().toTypedArray()
				TextArrayWritable(list)
			})
		}
	}

	class TextArrayWritable : ArrayWritable {
		constructor() : super(Text::class.java)
		constructor(array: Array<String>) : super(array)
		constructor(string: String) : super(arrayOf(string))

		override fun toString() = toStrings()?.contentDeepToString() ?: ""
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
						outputValueClass = TextArrayWritable::class.java

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
		class AnagramOutput<K, V>() : TextOutputFormat<K, V>() {
			override fun getRecordWriter(job: TaskAttemptContext): RecordWriter<K, V> =
					AnagramLineOutput(getDefaultWorkFile(job, "txt").let {
						it.getFileSystem(job.configuration).create(it, true)
					})

			private inner class AnagramLineOutput(out: DataOutputStream) : LineRecordWriter<K, V>(out, "") {
				override fun write(key: K, value: V) {
					super.write(key, value)
				}
			}
		}
	}
}

fun main(vararg args: String): Unit = exitProcess(ToolRunner.run(Anagrams(), args))
