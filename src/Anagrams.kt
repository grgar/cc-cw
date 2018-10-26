import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import java.util.*
import kotlin.system.exitProcess

class Anagrams : Configured(), Tool {
	class AnagramMapper : Mapper<Any, Text, Text, IntWritable>() {
		private val key = Text()
		private val value = IntWritable(1)

		override fun map(key: Any, value: Text, context: Context) {
			value
					.toString()
					.split("[^\\w]".toRegex())
					.asSequence()
					.filterNot { it.length == 1 || it.matches(".*\\d.*".toRegex()) }
					.map { it.toLowerCase(Locale.getDefault()).trim('_') }
					.filterNot { it.matches("(.)\\1+".toRegex()) }
					.forEach {
						context.write(this.key.apply { set(it) }, this.value)
					}
		}
	}

	class AnagramReducer : Reducer<Text, IntWritable, Text, IntWritable>() {
		private val result = IntWritable()

		override fun reduce(key: Text, values: MutableIterable<IntWritable>, context: Context) {
			val sum = values.fold(0) { acc, intWritable -> acc + intWritable.get() }
			result.set(sum)
			context.write(key, result)
		}
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
						outputValueClass = IntWritable::class.java

						outputFormatClass = TextOutputFormat::class.java
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
}

fun main(vararg args: String): Unit = exitProcess(ToolRunner.run(Anagrams(), args))
