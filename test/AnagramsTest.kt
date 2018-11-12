package com.georgegarside.coc105.anagrams

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Tests for the [Anagrams] class.
 */
internal class AnagramsTest {
	/**
	 * [Anagrams.AnagramReducer.anagram].
	 */
	private fun fold(left: Set<String>, right: Set<String>) = Anagrams.AnagramReducer().anagram(left, right)

	/**
	 * [fold] converting additional [String] to single element [Set].
	 */
	private infix fun Set<String>.with(right: String) = fold(this, setOf(right))

	/**
	 * [fold] converting a [String] to a single element [Set].
	 */
	private infix fun String.with(right: String) = fold(setOf(this), setOf(right))

	/**
	 * [setOf] of two strings.
	 */
	private infix fun String.and(right: String) = setOf(this, right)

	/**
	 * [assertEquals] to support non-set parameter overrides of this function name.
	 */
	private fun sets(expected: Set<String>, actual: Set<String>) = assertEquals(expected, actual)

	/**
	 * [assertEquals] converting a [String] to a single element [Set].
	 */
	private fun sets(expected: String, actual: Set<String>) = assertEquals(setOf(expected), actual)

	@Test
	internal fun `simple sets`() {
		sets("foo" and "bar", "foo" with "bar")
		sets("foo" and "foobar", "foo" with "foobar")
		sets("foo" and "foobar", "foobar" with "foo")
	}

	@Test
	internal fun `no duplicates`() {
		sets("foo", "foo" with "foo")
	}

	@Test
	internal fun `capital first letter`() {
		sets("Foo", "Foo" with "Foo")
		sets("foo", "foo" with "Foo")
		sets("foo", "Foo" with "foo")
		sets("foo" and "bar", "Foo" with "foo" with "bar")
		sets("foo" and "bar", "Foo" with "bar" with "foo")
		sets("foo" and "bar", "bar" with "Foo" with "foo")
		sets("foo" and "bar", "bar" with "foo" with "Foo")
	}

	@Test
	internal fun `apostrophe existing and new remains`() {
		sets("fo'o" and "bar", "fo'o" with "bar")
		sets("f'oo" and "bar", "f'oo" with "bar")
		sets("fo'o", "fo'o" with "fo'o")
	}

	@Test
	internal fun `apostrophe existing without new replaced`() {
		sets("foo", "fo'o" with "foo")
		sets("foo", "f'oo" with "foo")
	}

	@Test
	internal fun `apostrophe not existing with new remains`() {
		sets("foo", "foo" with "fo'o")
	}

	@Test
	internal fun `apostrophe differs`() {
		sets("foo", "f'oo" with "fo'o")
	}

	@Test
	internal fun `multiple apostrophe`() {
		sets("foo", "foo" with "f'o'o")
		sets("f'o'o", "f'o'o" with "f'o'o")
	}

	@Test
	internal fun `hyphenated word maintained`() {
		sets("fo-o", "fo-o" with "fo-o")
	}

	@Test
	internal fun `hyphenated word replaced`() {
		sets("foo", "foo" with "fo-o")
		sets("foo", "fo-o" with "foo")
	}

	@Test
	internal fun `accented characters anagrams`() {
		sets("fôö", "fôö" with "fôö")
		sets("fôö" and "föô", "fôö" with "föô")
		sets("foo" and "föô", "foo" with "föô")
	}

	@Test
	internal fun `capital removed keeping apostrophe`() {
		sets("fo'o", "Fo'o" with "fo'o")
		sets("fo'o", "fo'o" with "Fo'o")
	}

	@Test
	internal fun `apostrophe removed keeping capital`() {
		sets("Foo", "Foo" with "Fo'o")
		sets("Foo", "Fo'o" with "Foo")
	}

	@Test
	internal fun `both apostrophe and capital removed`() {
		sets("foo", "foo" with "Fo'o")
		sets("foo", "Foo" with "fo'o")
		sets("foo", "Fo'o" with "foo")
		sets("foo", "fo'o" with "foo")
	}

	@Test
	internal fun `both apostrophe and capital removed multiple steps`() {
		sets("foo", "foo" with "Foo" with "fo'o")
		sets("foo", "foo" with "fo'o" with "Foo")
		sets("foo", "Foo" with "fo'o" with "foo")
		sets("foo", "fo'o" with "Foo" with "foo")
		sets("foo", "fo'o" with "foo" with "Foo")
		sets("foo", "Foo" with "fo'o" with "foo")
	}

	@Test
	internal fun `both apostrophe and hyphen keeping both`() {
		sets("f-o'o", "f-o'o" with "f-o'o")
	}

	@Test
	internal fun `both apostrophe and hyphen one capital keeping apostrophe`() {
		sets("f-o'o", "f-o'o" with "F-o'o")
		sets("f-o'o", "F-o'o" with "f-o'o")
	}

	@Test
	internal fun `one apostrophe one hyphen keeping without`() {
		sets("foo", "fo'o" with "f-oo")
		sets("foo", "f'oo" with "f-oo")
	}

	@Test
	internal fun `multiple apostrophe matching`() {
		sets("foo", "f'o'o" with "fo'o")
		sets("foo", "f'oo" with "fo'o")
	}

	@Test
	internal fun `multiple separated hyphens`() {
		sets("foo", "f'o'o" with "f-oo")
		sets("foo", "f'oo" with "f-o-o")
		sets("foo", "f'oo" with "f-o-o")
	}
}
