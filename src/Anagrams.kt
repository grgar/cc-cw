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

/**
 * Performs parsing of text into tokens of words, then builds sets of words which anagram together.
 */
class Anagrams : Configured(), Tool {
	/**
	 * Mapper to extract words from text. Parses text, extracting all words which can be anagram of another word
	 * (i.e. ignores single letter words and words with all the same letter, which can never be anagram'd).
	 *
	 * Normalises text by splitting on specific word boundaries, removing numbers and words with numbers.
	 * • Input is line number of input text [LongWritable] and string in [Text].
	 * • Output is word with letters in alphabetical order as [Text] and [SetWritable] containing original form of word.
	 */
	class AnagramMapper : Mapper<LongWritable, Text, Text, SetWritable>() {
		/**
		 * Alphabetically sorted letters in word. Instance of [Text] reused for each [map] for performance.
		 */
		private var keyOut = Text()
		/**
		 * Set of original word. Instance of [SetWritable] reused for each [map] for performance.
		 */
		private var valOut = SetWritable()

		/**
		 * Wrapper for [Text] to make manipulation as string easier. Computed variable has value as the [toString] of text
		 * and setting variable will [set] value of Text.
		 */
		private var Text.value: String
			get() = toString()
			set(value) = set(value)

		/**
		 * Regex for matching a word from a string.
		 *
		 * By this regex, a word begins with Unicode letter follows with one or more Unicode letter or
		 * word joiner character defined in constant.
		 *
		 * By looking for specific first character and matching with +, only matches words with length 2 or greater,
		 * which is useful for later since words with only one character can never anagram with another word, so no
		 * need to waste computation checking words of length 1.
		 */
		private val wordRegex =
				"""[\pL\p{Mn}][\pL\p{Mn}$wordJoiners]+"""
						.toRegex(RegexOption.IGNORE_CASE)

		/**
		 * Mapper to extract words from [text]. Takes position being parsed as [index] (currently unused)
		 * and writes output to [context].
		 *
		 * Splits initially on spaces, double hyphens and underscores, then cleans up received words by retrieving matched
		 * groups using regex, with support for accented characters. Removes numbers and words with numbers.
		 */
		public override fun map(index: LongWritable, text: Text, context: Context) = text.value

				// Initial rudimentary splitting of input, leaving the more intensive processing for sequence-based parallelism
				.split(" |--|_".toRegex())

				// For parallelism performance, use sequence instead of list
				.asSequence()

				// Find and extract words. See wordRegex for details on how words are matched.
				.flatMap {
					// Find match of regex in string,
					// then return match if a match was found,
					// otherwise ignore string since it did not contain a word
					wordRegex.find(it)?.value
							?.let { word -> sequenceOf(word) }
							?: emptySequence()
				}

				// Additional filtering is performed to remove words containing digits and words with all the same letter
				.filterNot { it.matches(""".*\d.*|(.)\1+""".toRegex()) }

				// At this point, sequence contains a list of filtered words
				// For each word found, write out key and value to context
				.forEach { word ->

					// Key is normalised, alphabetically ordered letters in found word so that words can be compared as anagrams
					keyOut.value = word
							// Convert to lowercase (default locale is implied by method without argument)
							.toLowerCase()
							// Remove characters which are permitted in words but do not participate in the anagrams, e.g. apostrophe
							.filterNot { c -> wordJoiners.any { joiner -> c == joiner } }
							// Perform sorting of letters by representing string as array and sorting array of Char
							.toCharArray().sortedArray().joinToString(separator = "")

					// Value is original word
					valOut.value =
							// If word is written in uppercase only, lowercase the word before writing out as value
							if (word.all { it.isUpperCase() }) {
								setOf(word.toLowerCase())
							} else {
								setOf(word)
							}

					context.write(keyOut, valOut)
				}
	}

	/**
	 * Characters and operations based on [wordJoiners].
	 */
	companion object AnagramJoiners {
		/**
		 * Characters which do not participate in anagrams but which do not split words,
		 * i.e. the character can be included in a word but does not separate words.
		 */
		const val wordJoiners = "'’-"

		/**
		 * [Regex] for matching any character in [wordJoiners].
		 */
		private val wordJoinersRegex = """[$wordJoiners]""".toRegex()

		/**
		 * Remove all instances of characters in [wordJoiners] from a string.
		 */
		val removeJoiners = { word: String -> word.replace(wordJoinersRegex, "") }

		/**
		 * Get positions of the characters in [wordJoiners] from a string.
		 * Returns [Set] of [Pair] of [Int] representing the index of the joiner character and [Char] the matched character.
		 */
		val getPositions = { word: String ->
			// Find all instances of joiner characters
			wordJoinersRegex.findAll(word)
					.fold(setOf<Pair<Int, Char>>()) { acc, matchResult ->
						// Match range is either side of matched character, so since only one character can be matched, the first
						// of the range is the index of the character. Each result of match only contains one value since there
						// are no capturing groups, so the first value in the result is the entire match, containing one character
						acc + (matchResult.range.first to matchResult.value.first())
					}
		}
	}

	/**
	 * Partition based on length of word. This is for performance since words of different length will never anagram with
	 * each other. The more partitions available, the more different word lengths can be spread out across them and the
	 * less each partition will check keys for a word of different length.
	 */
	class AnagramPartitioner : Partitioner<Text, SetWritable>() {
		/**
		 * Choose a partition from the total number of available [partitions] for a word to be assigned into, based on the
		 * length of the [text]. Value of the [set] is not involved in choosing a partition.
		 */
		override fun getPartition(text: Text, set: SetWritable, partitions: Int) = text.length % partitions
	}

	/**
	 * Reducer for performing anagram operation on words with matching alphabetically ordered letters (i.e. words which
	 * are anagrams of each other or are the same word).
	 */
	class AnagramReducer : Reducer<Text, SetWritable, Text, SetWritable>() {
		/**
		 * Called by Hadoop for each reduce operation. Reduces [values] for the same [key]. Key is retained so reduction
		 * can be performed more than once, for example if this class is given as a Combiner to Hadoop. Output is written
		 * to [context].
		 */
		override fun reduce(key: Text, values: MutableIterable<SetWritable>, context: Context) {
			context.write(
					// Key remains unchanged (won't be printed in output though, but kept for potential optimisation later)
					key,
					// Output is SetWritable containing anagrams
					SetWritable(values
							// Given values may contain multiple SetWritable, each need their values anagram-merging
							.fold(setOf()) { acc, setWritable ->
								// Anagram the accumulator with the next set of words to be compared for anagrams
								acc anagramWith setWritable.value
							}
					)
			)
		}

		/**
		 * Wrapper for calling [anagram] as extension function on String.
		 */
		private infix fun Set<String>.anagramWith(right: Set<String>) = anagram(this, right)

		/**
		 * Wrapper for calling [AnagramJoiners.removeJoiners] as extension function on String.
		 */
		private fun String.removeJoiners() = removeJoiners(this)

		/**
		 * Wrapper for calling [AnagramJoiners.getPositions] as computed constant on String.
		 */
		private val String.joinerPositions
			get() = getPositions(this)

		/**
		 * Performs the anagram comparisons.
		 */
		fun anagram(left: Set<String>, right: Set<String>) = right.fold(left) { set, word ->
			// { Abc }, abc (lowercase of existing word) -> { abc } (remove Abc and add abc)
			set.find { it.toLowerCase() == word || it.removeJoiners() == word }
					?.let { set - it + word } ?:

			// { Foo }, Fo'o (apostrophe of existing word) -> { Foo } (leave set as existing)
			set.find { it == word.removeJoiners() }
					?.let { set } ?:

			// { Fo'o }, foo (lowercase and no apostrophe of existing word) -> { foo } (remove Fo'o and add foo)
			set.find { it.removeJoiners().toLowerCase() == word.toLowerCase() }
					?.let { set - it + word.toLowerCase() } ?:

			// { foo }, Fo'o (uppercase and apostrophe of existing word) -> { foo } (leave set as existing)
			set.find { it.toLowerCase() == word.removeJoiners().toLowerCase() }
					?.let { set - it + word.removeJoiners().toLowerCase() } ?:

			// { abc }, Abc (uppercase of existing word) -> { abc } (leave set as existing)
			set.firstOrNull { it == word.toLowerCase() || it == word.removeJoiners() }
					?.let { set } ?:

			// { fo'o }, f-oo (same word, differing with word joiners) -> {remove all word joiners and return original word}
			// { abc }, abc (word already exists as is) -> { abc } (leave set as existing)
			set.firstOrNull { it.removeJoiners().toLowerCase() == word.removeJoiners().toLowerCase() }
					?.let {
						// If all joiner positions match in the word, …
						if (it.joinerPositions == word.joinerPositions) {
							// … then the word is an existing match
							set
						} else {
							// … otherwise the existing word should be removed and the normalised version should be added
							set - it + word.removeJoiners()
						}
					} ?:

			// { abc }, cab (word does not exist) -> { abc, cab } (merge sets)
			set+word
		}
	}

	/**
	 * Implementation of [Set] which implements Hadoop's [Writable] interface, such that Hadoop can serialise and
	 * de-serialise the values within the Set.
	 */
	class SetWritable(var value: Set<String> = setOf()) : Writable {
		/**
		 * Reads [input] into [value]. First ingests length of set, then ingests UTF for each word in set.
		 */
		override fun readFields(input: DataInput): Unit =
				(0 until input.readInt())
						.fold<Int, Set<String>>(setOf()) { acc, _ ->
							acc + input.readUTF()
						}
						.let { value = it }

		/**
		 * Writes [value] into [output]. First egress is length of set, then each word in set is egress.
		 */
		override fun write(output: DataOutput) =
				value
						.also { output.writeInt(it.size) }
						.forEach { output.writeUTF(it) }

		/**
		 * String representation of set for Hadoop to write to file.
		 */
		override fun toString() =
				value.joinToString(prefix = "{ ", separator = ", ", postfix = " }")
	}

	/**
	 * Wrapper for running job with input and output paths from strings given as [args].
	 *
	 * @throws ArrayIndexOutOfBoundsException if less than 2 paths are given
	 */
	override fun run(vararg args: String) = run(Path(args[0]), Path(args[1]))

	/**
	 * Configures and runs [Anagrams] job.
	 */
	private fun run(input: Path, output: Path) = Job
			// Job name set by reflection
			.getInstance(conf, this::class.simpleName)
			// Default configuration is already applied from Anagrams class extending Configured
			.apply {
				setJarByClass(this@Anagrams::class.java)

				// Main steps in MapReduce
				mapperClass = Anagrams.AnagramMapper::class.java
				partitionerClass = Anagrams.AnagramPartitioner::class.java
				reducerClass = Anagrams.AnagramReducer::class.java

				// Hadoop IO
				inputFormatClass = TextInputFormat::class.java
				outputKeyClass = Text::class.java
				// Overriding default Hadoop classes
				outputValueClass = SetWritable::class.java
				outputFormatClass = AnagramOutput::class.java
			}
			// Set paths
			.also {
				FileInputFormat.addInputPath(it, input)
				FileOutputFormat.setOutputPath(it, output)
			}
			// Run job, calculating execution time
			.let { job ->
				// Manually calculate execution time since Job's startTime and endTime are 0 when not run in cluster, so
				// guarantee that regardless of how the job is executed a value will be returned for execution time
				val executionTime = measureTimeMillis { job.waitForCompletion(true) }
				println("Execution of ${this::class.qualifiedName ?: "Anagrams"} job took ${executionTime / 1000}s")
				0
			}

	/**
	 * Custom [TextOutputFormat] to override [getRecordWriter] specifying a custom line output.
	 */
	class AnagramOutput<K, V> : TextOutputFormat<K, V>() {
		/**
		 * Record writer to output [job] as .txt file with custom [AnagramLineRecordWriter].
		 */
		override fun getRecordWriter(job: TaskAttemptContext): RecordWriter<K, V> =
				AnagramLineRecordWriter(getDefaultWorkFile(job, ".txt").let {
					it.getFileSystem(job.configuration).create(it, true)
				})

		/**
		 * Custom record writer with [write] overriding output of line and specifying no separator between key and value.
		 */
		private inner class AnagramLineRecordWriter(out: DataOutputStream) :
				LineRecordWriter<K, V>(out, "") {
			/**
			 * Write [key] and [value] to line in file, only if value as Set contains more than one item.
			 * Also does not write key to final output.
			 */
			override fun write(key: K, value: V) {
				if (value is SetWritable && value.value.size == 1) return
				super.write(null, value)
			}
		}
	}
}

/**
 * Start execution of [Anagrams] using [ToolRunner] with given [args].
 */
fun main(vararg args: String): Unit = exitProcess(ToolRunner.run(Anagrams(), args))
