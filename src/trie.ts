export enum TrieResult {
  FAILED = "FAILED",
  PREFIX = "PREFIX",
  EXISTS = "EXISTS",
}

export type Trie = { [key: string]: Trie } & { 0?: true }

export function newTrie(keywords: Iterable<string>, trie: Trie = {}): Trie {
  for (const key of keywords) {
    let current = trie
    for (const char of key) {
      if (!(char in current)) {
        current[char] = {}
      }
      current = current[char] as Trie
    }
    current[0] = true
  }
  return trie
}

export function inTrie(trie: Trie, key: string): [TrieResult, Trie] {
  if (!key) {
    return [TrieResult.FAILED, trie]
  }

  let current = trie
  for (const char of key) {
    if (!(char in current)) {
      return [TrieResult.FAILED, current]
    }
    current = current[char] as Trie
  }

  if (0 in current) {
    return [TrieResult.EXISTS, current]
  }

  return [TrieResult.PREFIX, current]
}
