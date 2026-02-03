/**
 * Apache Solr dialect
 */

import { Dialect } from "../dialect.js"

export class SolrDialect extends Dialect {
  static override readonly name = "solr"
}

// Register dialect
Dialect.register(SolrDialect)
