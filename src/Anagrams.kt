import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import kotlin.system.exitProcess

class Anagrams : Tool {
	override fun run(p0: Array<out String>): Int = TODO("not implemented")
	override fun getConf(): Configuration = TODO("not implemented")
	override fun setConf(p0: Configuration) = TODO("not implemented")
}

fun main(vararg args: String): Unit = exitProcess(ToolRunner.run(Anagrams(), args))
