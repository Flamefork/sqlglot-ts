/**
 * Time format conversion utilities
 */

import { type Trie, TrieResult, inTrie, newTrie } from "./trie.js"

/**
 * Converts a date/time format string from one format to another using a mapping.
 * Uses trie-based longest prefix matching for efficient conversion.
 */
export function formatTime(
  format: string,
  mapping: Map<string, string>,
  trie?: Trie,
): string {
  if (mapping.size === 0) {
    return format
  }

  if (!trie) {
    trie = newTrie([...mapping.keys()])
  }

  let result = ""
  let i = 0

  while (i < format.length) {
    let currentTrie: Trie = trie
    let longestMatch = ""
    let longestMatchEnd = i

    // Find longest matching prefix using trie
    for (let j = i; j < format.length; j++) {
      const char = format[j]!
      const [trieResult, nextTrie] = inTrie(currentTrie, char)

      if (trieResult === TrieResult.FAILED) break

      currentTrie = nextTrie
      if (trieResult === TrieResult.EXISTS) {
        longestMatch = format.slice(i, j + 1)
        longestMatchEnd = j + 1
      }
    }

    if (longestMatch) {
      result += mapping.get(longestMatch) ?? longestMatch
      i = longestMatchEnd
    } else {
      result += format[i]
      i++
    }
  }

  return result
}
