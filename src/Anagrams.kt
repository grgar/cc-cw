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

		override fun map(key: Any, value: Text, context: Context) =
				StringTokenizer(value.toString()).iterator().forEach {
					this.key.set(it.toString())
					context.write(this.key, this.value)
				}
	}

	class AnagramReducer : Reducer<Text, IntWritable, Text, IntWritable>() {
		private val result = IntWritable()

		override fun reduce(key: Text, values: MutableIterable<IntWritable>, context: Context): Unit {
			val sum = values.fold(0) { acc, intWritable -> acc + intWritable.get() }
			result.set(sum)
			context.write(key, result)
		}
	}

	override fun run(vararg args: String): Int {
		if (args.size != 2) {
			ToolRunner.printGenericCommandUsage(System.err)
			RuntimeException("Usage: ${this::class.simpleName} <input> <output>")
			return -1
		}
		return run(Path(args[0]), Path(args[1]))
	}

	private fun run(input: Path, output: Path): Int {
		val job = Job.getInstance(conf, this::class.simpleName).apply {
			setJarByClass(this@Anagrams::class.java)

			mapperClass = Anagrams.AnagramMapper::class.java
			reducerClass = Anagrams.AnagramReducer::class.java
			numReduceTasks = 1

			inputFormatClass = TextInputFormat::class.java

			outputKeyClass = Text::class.java
			outputValueClass = IntWritable::class.java

			outputFormatClass = TextOutputFormat::class.java
		}

		FileInputFormat.addInputPath(job, input)
		FileOutputFormat.setOutputPath(job, output)

		FileSystem.get(conf).apply {
			if (exists(output)) delete(output, true)
		}

		return if (job.waitForCompletion(true)) 0 else 1
	}
}

fun main(vararg args: String): Unit = exitProcess(ToolRunner.run(Anagrams(), args))
