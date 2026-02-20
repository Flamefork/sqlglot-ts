/**
 * AUTO-GENERATED - DO NOT EDIT
 * Generated from sqlglot/sqlglot/expressions.py
 * Run: npm run generate
 */

import { Expression, type ExpressionClass, maybeParse, _applyBuilder, _applyListBuilder, _applyChildListBuilder, _applyConjunctionBuilder } from './expression-base.js';

function camelToSnakeCase(s: string): string {
  return s.replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase();
}

export class Condition extends Expression {
  get key(): string { return 'condition'; }
  static readonly className: string = 'Condition';
  and_(...expressions: (string | Expression)[]): Expression {
    const parsed = expressions.map(e => typeof e === 'string' ? Expression.parseImpl(e) : e);
    let result: Expression = this;
    for (const expr of parsed) { result = new And({ this: result, expression: expr }); }
    return result;
  }
  or_(...expressions: (string | Expression)[]): Expression {
    const parsed = expressions.map(e => typeof e === 'string' ? Expression.parseImpl(e) : e);
    let result: Expression = this;
    for (const expr of parsed) { result = new Or({ this: result, expression: expr }); }
    return result;
  }
  not_(): Not { return new Not({ this: this }); }
  eq(other: unknown): EQ { return this._binop(EQ, other) as EQ; }
  neq(other: unknown): NEQ { return this._binop(NEQ, other) as NEQ; }
  is_(other: unknown): Is { return this._binop(Is, other) as Is; }
  like(other: unknown): Like { return this._binop(Like, other) as Like; }
  ilike(other: unknown): ILike { return this._binop(ILike, other) as ILike; }
  rlike(other: unknown): RegexpLike { return this._binop(RegexpLike, other) as RegexpLike; }
  asc(nullsFirst = true): Ordered { return new Ordered({ this: this, nulls_first: nullsFirst }); }
  desc(nullsFirst = false): Ordered { return new Ordered({ this: this, desc: true, nulls_first: nullsFirst }); }
}

export class Predicate extends Condition {
  override get key(): string { return 'predicate'; }
  static readonly className: string = 'Predicate';
}

export class DerivedTable extends Expression {
  get key(): string { return 'derivedtable'; }
  static readonly className: string = 'DerivedTable';
}

export class Query extends Expression {
  get key(): string { return 'query'; }
  static readonly className: string = 'Query';
  subquery(alias?: string): Subquery {
    const sub = new Subquery({ this: this });
    if (alias) { sub.set('alias', new TableAlias({ this: new Identifier({ this: alias }) })); }
    return sub;
  }
  limit(expression: string | Expression, options?: { copy?: boolean; dialect?: string }): this {
    return _applyBuilder(expression, this, 'limit', { copy: options?.copy ?? true, into: Limit, prefix: 'LIMIT', dialect: options?.dialect }) as this;
  }
  offset(expression: string | Expression, options?: { copy?: boolean; dialect?: string }): this {
    return _applyBuilder(expression, this, 'offset', { copy: options?.copy ?? true, into: Offset, prefix: 'OFFSET', dialect: options?.dialect }) as this;
  }
  orderBy(...expressions: (string | Expression)[]): this {
    return _applyChildListBuilder(expressions, this, 'order', { copy: true, into: Order, prefix: 'ORDER BY' }) as this;
  }
  where(...expressions: (string | Expression)[]): this {
    return _applyConjunctionBuilder(expressions, this, 'where', { copy: true, into: Where, append: true }) as this;
  }
  with_(alias: string | Expression, as_: string | Expression, options?: { recursive?: boolean; materialized?: boolean; append?: boolean; copy?: boolean; dialect?: string; scalar?: boolean }): this {
    const aliasExpr = maybeParse(alias, { dialect: options?.dialect, into: TableAlias });
    let asExpr = maybeParse(as_, { dialect: options?.dialect, copy: options?.copy });
    if (options?.scalar && !(asExpr instanceof Subquery)) { asExpr = new Subquery({ this: asExpr }); }
    const cte = new CTE({ this: asExpr, alias: aliasExpr, materialized: options?.materialized, scalar: options?.scalar });
    return _applyChildListBuilder([cte], this, 'with_', { append: options?.append ?? true, copy: options?.copy ?? true, into: With, properties: options?.recursive ? { recursive: options.recursive } : {} }) as this;
  }
  union(...expressions: (string | Expression)[]): Union {
    const parsed = expressions.map(e => maybeParse(e));
    let result: Expression = this as Expression;
    for (const expr of parsed) { result = new Union({ this: result, expression: expr, distinct: true }); }
    return result as Union;
  }
  intersect(...expressions: (string | Expression)[]): Intersect {
    const parsed = expressions.map(e => maybeParse(e));
    let result: Expression = this as Expression;
    for (const expr of parsed) { result = new Intersect({ this: result, expression: expr, distinct: true }); }
    return result as Intersect;
  }
  except_(...expressions: (string | Expression)[]): Except {
    const parsed = expressions.map(e => maybeParse(e));
    let result: Expression = this as Expression;
    for (const expr of parsed) { result = new Except({ this: result, expression: expr, distinct: true }); }
    return result as Except;
  }
}

export class UDTF extends DerivedTable {
  override get key(): string { return 'udtf'; }
  static readonly className: string = 'UDTF';
}

export class Cache extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'lazy': false, 'options': false, 'expression': false };
  get key(): string { return 'cache'; }
  static readonly className: string = 'Cache';
}

export class Uncache extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'exists': false };
  get key(): string { return 'uncache'; }
  static readonly className: string = 'Uncache';
}

export class Refresh extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': true };
  get key(): string { return 'refresh'; }
  static readonly className: string = 'Refresh';
}

export class DDL extends Expression {
  get key(): string { return 'ddl'; }
  static readonly className: string = 'DDL';
}

export class LockingStatement extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'lockingstatement'; }
  static readonly className: string = 'LockingStatement';
}

export class DML extends Expression {
  get key(): string { return 'dml'; }
  static readonly className: string = 'DML';
}

export class Create extends DDL {
  static readonly argTypes: Record<string, boolean> = { 'with_': false, 'this': true, 'kind': true, 'expression': false, 'exists': false, 'properties': false, 'replace': false, 'refresh': false, 'unique': false, 'indexes': false, 'no_schema_binding': false, 'begin': false, 'end': false, 'clone': false, 'concurrently': false, 'clustered': false };
  override get key(): string { return 'create'; }
  static readonly className: string = 'Create';
}

export class SequenceProperties extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'increment': false, 'minvalue': false, 'maxvalue': false, 'cache': false, 'start': false, 'owned': false, 'options': false };
  get key(): string { return 'sequenceproperties'; }
  static readonly className: string = 'SequenceProperties';
}

export class TruncateTable extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'is_database': false, 'exists': false, 'only': false, 'cluster': false, 'identity': false, 'option': false, 'partition': false };
  get key(): string { return 'truncatetable'; }
  static readonly className: string = 'TruncateTable';
}

export class Clone extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'shallow': false, 'copy': false };
  get key(): string { return 'clone'; }
  static readonly className: string = 'Clone';
}

export class Describe extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'style': false, 'kind': false, 'expressions': false, 'partition': false, 'format': false, 'as_json': false };
  get key(): string { return 'describe'; }
  static readonly className: string = 'Describe';
}

export class Attach extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'exists': false, 'expressions': false };
  get key(): string { return 'attach'; }
  static readonly className: string = 'Attach';
}

export class Detach extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'exists': false };
  get key(): string { return 'detach'; }
  static readonly className: string = 'Detach';
}

export class Install extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'from_': false, 'force': false };
  get key(): string { return 'install'; }
  static readonly className: string = 'Install';
}

export class Summarize extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'table': false };
  get key(): string { return 'summarize'; }
  static readonly className: string = 'Summarize';
}

export class Kill extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': false };
  get key(): string { return 'kill'; }
  static readonly className: string = 'Kill';
}

export class Pragma extends Expression {
  get key(): string { return 'pragma'; }
  static readonly className: string = 'Pragma';
}

export class Declare extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'declare'; }
  static readonly className: string = 'Declare';
}

export class DeclareItem extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': false, 'default': false };
  get key(): string { return 'declareitem'; }
  static readonly className: string = 'DeclareItem';
}

export class Set extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'unset': false, 'tag': false };
  get key(): string { return 'set'; }
  static readonly className: string = 'Set';
}

export class Heredoc extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'tag': false };
  get key(): string { return 'heredoc'; }
  static readonly className: string = 'Heredoc';
}

export class SetItem extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': false, 'kind': false, 'collate': false, 'global_': false };
  get key(): string { return 'setitem'; }
  static readonly className: string = 'SetItem';
}

export class QueryBand extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'scope': false, 'update': false };
  get key(): string { return 'queryband'; }
  static readonly className: string = 'QueryBand';
}

export class Show extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'history': false, 'terse': false, 'target': false, 'offset': false, 'starts_with': false, 'limit': false, 'from_': false, 'like': false, 'where': false, 'db': false, 'scope': false, 'scope_kind': false, 'full': false, 'mutex': false, 'query': false, 'channel': false, 'global_': false, 'log': false, 'position': false, 'types': false, 'privileges': false, 'for_table': false, 'for_group': false, 'for_user': false, 'for_role': false, 'into_outfile': false, 'json': false };
  get key(): string { return 'show'; }
  static readonly className: string = 'Show';
}

export class UserDefinedFunction extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'wrapped': false };
  get key(): string { return 'userdefinedfunction'; }
  static readonly className: string = 'UserDefinedFunction';
}

export class CharacterSet extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'default': false };
  get key(): string { return 'characterset'; }
  static readonly className: string = 'CharacterSet';
}

export class RecursiveWithSearch extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'kind': true, 'this': true, 'expression': true, 'using': false };
  get key(): string { return 'recursivewithsearch'; }
  static readonly className: string = 'RecursiveWithSearch';
}

export class With extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'recursive': false, 'search': false };
  get key(): string { return 'with'; }
  static readonly className: string = 'With';
}

export class WithinGroup extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  get key(): string { return 'withingroup'; }
  static readonly className: string = 'WithinGroup';
}

export class CTE extends DerivedTable {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alias': true, 'scalar': false, 'materialized': false, 'key_expressions': false };
  override get key(): string { return 'cte'; }
  static readonly className: string = 'CTE';
}

export class ProjectionDef extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'projectiondef'; }
  static readonly className: string = 'ProjectionDef';
}

export class TableAlias extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'columns': false };
  get key(): string { return 'tablealias'; }
  static readonly className: string = 'TableAlias';
}

export class BitString extends Condition {
  override get key(): string { return 'bitstring'; }
  static readonly className: string = 'BitString';
}

export class HexString extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'is_integer': false };
  override get key(): string { return 'hexstring'; }
  static readonly className: string = 'HexString';
}

export class ByteString extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'is_bytes': false };
  override get key(): string { return 'bytestring'; }
  static readonly className: string = 'ByteString';
}

export class RawString extends Condition {
  override get key(): string { return 'rawstring'; }
  static readonly className: string = 'RawString';
}

export class UnicodeString extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'escape': false };
  override get key(): string { return 'unicodestring'; }
  static readonly className: string = 'UnicodeString';
}

export class Column extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'table': false, 'db': false, 'catalog': false, 'join_mark': false };
  override get key(): string { return 'column'; }
  static readonly className: string = 'Column';
  get name(): string { return this.text('this'); }
  get table(): string { return this.text('table'); }
  get db(): string { return this.text('db'); }
  get catalog(): string { return this.text('catalog'); }
  override get isStar(): boolean { const thisVal = this.args.this; return thisVal instanceof Expression && thisVal.key === 'star'; }
}

export class Pseudocolumn extends Column {
  override get key(): string { return 'pseudocolumn'; }
  static readonly className: string = 'Pseudocolumn';
}

export class ColumnPosition extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'position': true };
  get key(): string { return 'columnposition'; }
  static readonly className: string = 'ColumnPosition';
}

export class ColumnDef extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': false, 'constraints': false, 'exists': false, 'position': false, 'default': false, 'output': false };
  get key(): string { return 'columndef'; }
  static readonly className: string = 'ColumnDef';
}

export class AlterColumn extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'dtype': false, 'collate': false, 'using': false, 'default': false, 'drop': false, 'comment': false, 'allow_null': false, 'visible': false, 'rename_to': false };
  get key(): string { return 'altercolumn'; }
  static readonly className: string = 'AlterColumn';
}

export class AlterIndex extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'visible': true };
  get key(): string { return 'alterindex'; }
  static readonly className: string = 'AlterIndex';
}

export class AlterDistStyle extends Expression {
  get key(): string { return 'alterdiststyle'; }
  static readonly className: string = 'AlterDistStyle';
}

export class AlterSortKey extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': false, 'compound': false };
  get key(): string { return 'altersortkey'; }
  static readonly className: string = 'AlterSortKey';
}

export class AlterSet extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'option': false, 'tablespace': false, 'access_method': false, 'file_format': false, 'copy_options': false, 'tag': false, 'location': false, 'serde': false };
  get key(): string { return 'alterset'; }
  static readonly className: string = 'AlterSet';
}

export class RenameColumn extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'to': true, 'exists': false };
  get key(): string { return 'renamecolumn'; }
  static readonly className: string = 'RenameColumn';
}

export class AlterRename extends Expression {
  get key(): string { return 'alterrename'; }
  static readonly className: string = 'AlterRename';
}

export class SwapTable extends Expression {
  get key(): string { return 'swaptable'; }
  static readonly className: string = 'SwapTable';
}

export class Comment extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': true, 'expression': true, 'exists': false, 'materialized': false };
  get key(): string { return 'comment'; }
  static readonly className: string = 'Comment';
}

export class Comprehension extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'position': false, 'iterator': true, 'condition': false };
  get key(): string { return 'comprehension'; }
  static readonly className: string = 'Comprehension';
}

export class MergeTreeTTLAction extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'delete': false, 'recompress': false, 'to_disk': false, 'to_volume': false };
  get key(): string { return 'mergetreettlaction'; }
  static readonly className: string = 'MergeTreeTTLAction';
}

export class MergeTreeTTL extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'where': false, 'group': false, 'aggregates': false };
  get key(): string { return 'mergetreettl'; }
  static readonly className: string = 'MergeTreeTTL';
}

export class IndexConstraintOption extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'key_block_size': false, 'using': false, 'parser': false, 'comment': false, 'visible': false, 'engine_attr': false, 'secondary_engine_attr': false };
  get key(): string { return 'indexconstraintoption'; }
  static readonly className: string = 'IndexConstraintOption';
}

export class ColumnConstraint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'kind': true };
  get key(): string { return 'columnconstraint'; }
  static readonly className: string = 'ColumnConstraint';
}

export class ColumnConstraintKind extends Expression {
  get key(): string { return 'columnconstraintkind'; }
  static readonly className: string = 'ColumnConstraintKind';
}

export class AutoIncrementColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'autoincrementcolumnconstraint'; }
  static readonly className: string = 'AutoIncrementColumnConstraint';
}

export class ZeroFillColumnConstraint extends ColumnConstraint {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'zerofillcolumnconstraint'; }
  static readonly className: string = 'ZeroFillColumnConstraint';
}

export class PeriodForSystemTimeConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'periodforsystemtimeconstraint'; }
  static readonly className: string = 'PeriodForSystemTimeConstraint';
}

export class CaseSpecificColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'not_': true };
  override get key(): string { return 'casespecificcolumnconstraint'; }
  static readonly className: string = 'CaseSpecificColumnConstraint';
}

export class CharacterSetColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'charactersetcolumnconstraint'; }
  static readonly className: string = 'CharacterSetColumnConstraint';
}

export class CheckColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'enforced': false };
  override get key(): string { return 'checkcolumnconstraint'; }
  static readonly className: string = 'CheckColumnConstraint';
}

export class ClusteredColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'clusteredcolumnconstraint'; }
  static readonly className: string = 'ClusteredColumnConstraint';
}

export class CollateColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'collatecolumnconstraint'; }
  static readonly className: string = 'CollateColumnConstraint';
}

export class CommentColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'commentcolumnconstraint'; }
  static readonly className: string = 'CommentColumnConstraint';
}

export class CompressColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'compresscolumnconstraint'; }
  static readonly className: string = 'CompressColumnConstraint';
}

export class DateFormatColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'dateformatcolumnconstraint'; }
  static readonly className: string = 'DateFormatColumnConstraint';
}

export class DefaultColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'defaultcolumnconstraint'; }
  static readonly className: string = 'DefaultColumnConstraint';
}

export class EncodeColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'encodecolumnconstraint'; }
  static readonly className: string = 'EncodeColumnConstraint';
}

export class ExcludeColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'excludecolumnconstraint'; }
  static readonly className: string = 'ExcludeColumnConstraint';
}

export class EphemeralColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'ephemeralcolumnconstraint'; }
  static readonly className: string = 'EphemeralColumnConstraint';
}

export class WithOperator extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'op': true };
  get key(): string { return 'withoperator'; }
  static readonly className: string = 'WithOperator';
}

export class GeneratedAsIdentityColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expression': false, 'on_null': false, 'start': false, 'increment': false, 'minvalue': false, 'maxvalue': false, 'cycle': false, 'order': false };
  override get key(): string { return 'generatedasidentitycolumnconstraint'; }
  static readonly className: string = 'GeneratedAsIdentityColumnConstraint';
}

export class GeneratedAsRowColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'start': false, 'hidden': false };
  override get key(): string { return 'generatedasrowcolumnconstraint'; }
  static readonly className: string = 'GeneratedAsRowColumnConstraint';
}

export class IndexColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': false, 'kind': false, 'index_type': false, 'options': false, 'expression': false, 'granularity': false };
  override get key(): string { return 'indexcolumnconstraint'; }
  static readonly className: string = 'IndexColumnConstraint';
}

export class InlineLengthColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'inlinelengthcolumnconstraint'; }
  static readonly className: string = 'InlineLengthColumnConstraint';
}

export class NonClusteredColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'nonclusteredcolumnconstraint'; }
  static readonly className: string = 'NonClusteredColumnConstraint';
}

export class NotForReplicationColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'notforreplicationcolumnconstraint'; }
  static readonly className: string = 'NotForReplicationColumnConstraint';
}

export class MaskingPolicyColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  override get key(): string { return 'maskingpolicycolumnconstraint'; }
  static readonly className: string = 'MaskingPolicyColumnConstraint';
}

export class NotNullColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'allow_null': false };
  override get key(): string { return 'notnullcolumnconstraint'; }
  static readonly className: string = 'NotNullColumnConstraint';
}

export class OnUpdateColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'onupdatecolumnconstraint'; }
  static readonly className: string = 'OnUpdateColumnConstraint';
}

export class PrimaryKeyColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'desc': false, 'options': false };
  override get key(): string { return 'primarykeycolumnconstraint'; }
  static readonly className: string = 'PrimaryKeyColumnConstraint';
}

export class TitleColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'titlecolumnconstraint'; }
  static readonly className: string = 'TitleColumnConstraint';
}

export class UniqueColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'index_type': false, 'on_conflict': false, 'nulls': false, 'options': false };
  override get key(): string { return 'uniquecolumnconstraint'; }
  static readonly className: string = 'UniqueColumnConstraint';
}

export class UppercaseColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'uppercasecolumnconstraint'; }
  static readonly className: string = 'UppercaseColumnConstraint';
}

export class WatermarkColumnConstraint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'watermarkcolumnconstraint'; }
  static readonly className: string = 'WatermarkColumnConstraint';
}

export class PathColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'pathcolumnconstraint'; }
  static readonly className: string = 'PathColumnConstraint';
}

export class ProjectionPolicyColumnConstraint extends ColumnConstraintKind {
  override get key(): string { return 'projectionpolicycolumnconstraint'; }
  static readonly className: string = 'ProjectionPolicyColumnConstraint';
}

export class ComputedColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'persisted': false, 'not_null': false, 'data_type': false };
  override get key(): string { return 'computedcolumnconstraint'; }
  static readonly className: string = 'ComputedColumnConstraint';
}

export class InOutColumnConstraint extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'input_': false, 'output': false, 'variadic': false };
  override get key(): string { return 'inoutcolumnconstraint'; }
  static readonly className: string = 'InOutColumnConstraint';
}

export class Constraint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  get key(): string { return 'constraint'; }
  static readonly className: string = 'Constraint';
}

export class Delete extends DML {
  static readonly argTypes: Record<string, boolean> = { 'with_': false, 'this': false, 'using': false, 'where': false, 'returning': false, 'order': false, 'limit': false, 'tables': false, 'cluster': false };
  override get key(): string { return 'delete'; }
  static readonly className: string = 'Delete';
}

export class Drop extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'kind': false, 'expressions': false, 'exists': false, 'temporary': false, 'materialized': false, 'cascade': false, 'constraints': false, 'purge': false, 'cluster': false, 'concurrently': false };
  get key(): string { return 'drop'; }
  static readonly className: string = 'Drop';
}

export class Export extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'connection': false, 'options': true };
  get key(): string { return 'export'; }
  static readonly className: string = 'Export';
}

export class Filter extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'filter'; }
  static readonly className: string = 'Filter';
}

export class Check extends Expression {
  get key(): string { return 'check'; }
  static readonly className: string = 'Check';
}

export class Changes extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'information': true, 'at_before': false, 'end': false };
  get key(): string { return 'changes'; }
  static readonly className: string = 'Changes';
}

export class Connect extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'start': false, 'connect': true, 'nocycle': false };
  get key(): string { return 'connect'; }
  static readonly className: string = 'Connect';
}

export class CopyParameter extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'expressions': false };
  get key(): string { return 'copyparameter'; }
  static readonly className: string = 'CopyParameter';
}

export class Copy extends DML {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': true, 'files': false, 'credentials': false, 'format': false, 'params': false };
  override get key(): string { return 'copy'; }
  static readonly className: string = 'Copy';
}

export class Credentials extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'credentials': false, 'encryption': false, 'storage': false, 'iam_role': false, 'region': false };
  get key(): string { return 'credentials'; }
  static readonly className: string = 'Credentials';
}

export class Prior extends Expression {
  get key(): string { return 'prior'; }
  static readonly className: string = 'Prior';
}

export class Directory extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'local': false, 'row_format': false };
  get key(): string { return 'directory'; }
  static readonly className: string = 'Directory';
}

export class DirectoryStage extends Expression {
  get key(): string { return 'directorystage'; }
  static readonly className: string = 'DirectoryStage';
}

export class ForeignKey extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'reference': false, 'delete': false, 'update': false, 'options': false };
  get key(): string { return 'foreignkey'; }
  static readonly className: string = 'ForeignKey';
}

export class ColumnPrefix extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'columnprefix'; }
  static readonly className: string = 'ColumnPrefix';
}

export class PrimaryKey extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': true, 'options': false, 'include': false };
  get key(): string { return 'primarykey'; }
  static readonly className: string = 'PrimaryKey';
}

export class Into extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'temporary': false, 'unlogged': false, 'bulk_collect': false, 'expressions': false };
  get key(): string { return 'into'; }
  static readonly className: string = 'Into';
}

export class From extends Expression {
  get key(): string { return 'from'; }
  static readonly className: string = 'From';
}

export class Having extends Expression {
  get key(): string { return 'having'; }
  static readonly className: string = 'Having';
}

export class Hint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'hint'; }
  static readonly className: string = 'Hint';
}

export class JoinHint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  get key(): string { return 'joinhint'; }
  static readonly className: string = 'JoinHint';
}

export class Identifier extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'quoted': false, 'global_': false, 'temporary': false };
  get key(): string { return 'identifier'; }
  static readonly className: string = 'Identifier';
  get name(): string { return this.text('this'); }
  get quoted(): boolean { return !!this.args['quoted']; }
}

export class Opclass extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'opclass'; }
  static readonly className: string = 'Opclass';
}

export class Index extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'table': false, 'unique': false, 'primary': false, 'amp': false, 'params': false };
  get key(): string { return 'index'; }
  static readonly className: string = 'Index';
}

export class IndexParameters extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'using': false, 'include': false, 'columns': false, 'with_storage': false, 'partition_by': false, 'tablespace': false, 'where': false, 'on': false };
  get key(): string { return 'indexparameters'; }
  static readonly className: string = 'IndexParameters';
}

// Also extends: DML
export class Insert extends DDL {
  static readonly argTypes: Record<string, boolean> = { 'hint': false, 'with_': false, 'is_function': false, 'this': false, 'expression': false, 'conflict': false, 'returning': false, 'overwrite': false, 'exists': false, 'alternative': false, 'where': false, 'ignore': false, 'by_name': false, 'stored': false, 'partition': false, 'settings': false, 'source': false, 'default': false };
  override get key(): string { return 'insert'; }
  static readonly className: string = 'Insert';
}

export class ConditionalInsert extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'else_': false };
  get key(): string { return 'conditionalinsert'; }
  static readonly className: string = 'ConditionalInsert';
}

export class MultitableInserts extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'kind': true, 'source': true };
  get key(): string { return 'multitableinserts'; }
  static readonly className: string = 'MultitableInserts';
}

export class OnConflict extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'duplicate': false, 'expressions': false, 'action': false, 'conflict_keys': false, 'index_predicate': false, 'constraint': false, 'where': false };
  get key(): string { return 'onconflict'; }
  static readonly className: string = 'OnConflict';
}

export class OnCondition extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'error': false, 'empty': false, 'null': false };
  get key(): string { return 'oncondition'; }
  static readonly className: string = 'OnCondition';
}

export class Returning extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'into': false };
  get key(): string { return 'returning'; }
  static readonly className: string = 'Returning';
}

export class Introducer extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'introducer'; }
  static readonly className: string = 'Introducer';
}

export class National extends Expression {
  get key(): string { return 'national'; }
  static readonly className: string = 'National';
}

export class LoadData extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'local': false, 'overwrite': false, 'inpath': true, 'partition': false, 'input_format': false, 'serde': false };
  get key(): string { return 'loaddata'; }
  static readonly className: string = 'LoadData';
}

export class Partition extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'subpartition': false };
  get key(): string { return 'partition'; }
  static readonly className: string = 'Partition';
}

export class PartitionRange extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'expressions': false };
  get key(): string { return 'partitionrange'; }
  static readonly className: string = 'PartitionRange';
}

export class PartitionId extends Expression {
  get key(): string { return 'partitionid'; }
  static readonly className: string = 'PartitionId';
}

export class Fetch extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'direction': false, 'count': false, 'limit_options': false };
  get key(): string { return 'fetch'; }
  static readonly className: string = 'Fetch';
}

export class Grant extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'privileges': true, 'kind': false, 'securable': true, 'principals': true, 'grant_option': false };
  get key(): string { return 'grant'; }
  static readonly className: string = 'Grant';
}

export class Revoke extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'cascade': false };
  get key(): string { return 'revoke'; }
  static readonly className: string = 'Revoke';
}

export class Group extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'grouping_sets': false, 'cube': false, 'rollup': false, 'totals': false, 'all': false };
  get key(): string { return 'group'; }
  static readonly className: string = 'Group';
}

export class Cube extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  get key(): string { return 'cube'; }
  static readonly className: string = 'Cube';
}

export class Rollup extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  get key(): string { return 'rollup'; }
  static readonly className: string = 'Rollup';
}

export class GroupingSets extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'groupingsets'; }
  static readonly className: string = 'GroupingSets';
}

export class Lambda extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'colon': false };
  get key(): string { return 'lambda'; }
  static readonly className: string = 'Lambda';
}

export class Limit extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expression': true, 'offset': false, 'limit_options': false, 'expressions': false };
  get key(): string { return 'limit'; }
  static readonly className: string = 'Limit';
}

export class LimitOptions extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'percent': false, 'rows': false, 'with_ties': false };
  get key(): string { return 'limitoptions'; }
  static readonly className: string = 'LimitOptions';
}

export class Literal extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'is_string': true };
  override get key(): string { return 'literal'; }
  static readonly className: string = 'Literal';
  get isString(): boolean { return !!this.args['is_string']; }
  get isNumber(): boolean { return !this.isString; }
  get value(): string | number {
    const val = this.args['this'];
    if (typeof val === 'string') { return this.isNumber ? parseFloat(val) : val; }
    if (typeof val === 'number') { return val; }
    return '';
  }
  static string(val: string): Literal { return new Literal({ this: val, is_string: true }); }
  static number(val: number | string): Literal { return new Literal({ this: `${val}`, is_string: false }); }
}

export class Join extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'on': false, 'side': false, 'kind': false, 'using': false, 'method': false, 'global_': false, 'hint': false, 'match_condition': false, 'directed': false, 'expressions': false, 'pivots': false };
  get key(): string { return 'join'; }
  static readonly className: string = 'Join';
}

export class Lateral extends UDTF {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'view': false, 'outer': false, 'alias': false, 'cross_apply': false, 'ordinality': false };
  override get key(): string { return 'lateral'; }
  static readonly className: string = 'Lateral';
}

export class TableFromRows extends UDTF {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alias': false, 'joins': false, 'pivots': false, 'sample': false };
  override get key(): string { return 'tablefromrows'; }
  static readonly className: string = 'TableFromRows';
}

export class MatchRecognizeMeasure extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'window_frame': false };
  get key(): string { return 'matchrecognizemeasure'; }
  static readonly className: string = 'MatchRecognizeMeasure';
}

export class MatchRecognize extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'partition_by': false, 'order': false, 'measures': false, 'rows': false, 'after': false, 'pattern': false, 'define': false, 'alias': false };
  get key(): string { return 'matchrecognize'; }
  static readonly className: string = 'MatchRecognize';
}

export class Final extends Expression {
  get key(): string { return 'final'; }
  static readonly className: string = 'Final';
}

export class Offset extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expression': true, 'expressions': false };
  get key(): string { return 'offset'; }
  static readonly className: string = 'Offset';
}

export class Order extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': true, 'siblings': false };
  get key(): string { return 'order'; }
  static readonly className: string = 'Order';
}

export class WithFill extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'from_': false, 'to': false, 'step': false, 'interpolate': false };
  get key(): string { return 'withfill'; }
  static readonly className: string = 'WithFill';
}

export class Cluster extends Order {
  override get key(): string { return 'cluster'; }
  static readonly className: string = 'Cluster';
}

export class Distribute extends Order {
  override get key(): string { return 'distribute'; }
  static readonly className: string = 'Distribute';
}

export class Sort extends Order {
  override get key(): string { return 'sort'; }
  static readonly className: string = 'Sort';
}

export class Ordered extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'desc': false, 'nulls_first': true, 'with_fill': false };
  get key(): string { return 'ordered'; }
  static readonly className: string = 'Ordered';
  get desc(): boolean { return !!this.args['desc']; }
  get nullsFirst(): boolean | undefined {
    const val = this.args['nulls_first'];
    return typeof val === 'boolean' ? val : undefined;
  }
}

export class Property extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'value': true };
  get key(): string { return 'property'; }
  static readonly className: string = 'Property';
}

export class GrantPrivilege extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  get key(): string { return 'grantprivilege'; }
  static readonly className: string = 'GrantPrivilege';
}

export class GrantPrincipal extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': false };
  get key(): string { return 'grantprincipal'; }
  static readonly className: string = 'GrantPrincipal';
}

export class AllowedValuesProperty extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'allowedvaluesproperty'; }
  static readonly className: string = 'AllowedValuesProperty';
}

export class AlgorithmProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'algorithmproperty'; }
  static readonly className: string = 'AlgorithmProperty';
}

export class AutoIncrementProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'autoincrementproperty'; }
  static readonly className: string = 'AutoIncrementProperty';
}

export class AutoRefreshProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'autorefreshproperty'; }
  static readonly className: string = 'AutoRefreshProperty';
}

export class BackupProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'backupproperty'; }
  static readonly className: string = 'BackupProperty';
}

export class BuildProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'buildproperty'; }
  static readonly className: string = 'BuildProperty';
}

export class BlockCompressionProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'autotemp': false, 'always': false, 'default': false, 'manual': false, 'never': false };
  override get key(): string { return 'blockcompressionproperty'; }
  static readonly className: string = 'BlockCompressionProperty';
}

export class CharacterSetProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'default': true };
  override get key(): string { return 'charactersetproperty'; }
  static readonly className: string = 'CharacterSetProperty';
}

export class ChecksumProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'on': false, 'default': false };
  override get key(): string { return 'checksumproperty'; }
  static readonly className: string = 'ChecksumProperty';
}

export class CollateProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'default': false };
  override get key(): string { return 'collateproperty'; }
  static readonly className: string = 'CollateProperty';
}

export class CopyGrantsProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'copygrantsproperty'; }
  static readonly className: string = 'CopyGrantsProperty';
}

export class DataBlocksizeProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'size': false, 'units': false, 'minimum': false, 'maximum': false, 'default': false };
  override get key(): string { return 'datablocksizeproperty'; }
  static readonly className: string = 'DataBlocksizeProperty';
}

export class DataDeletionProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'on': true, 'filter_column': false, 'retention_period': false };
  override get key(): string { return 'datadeletionproperty'; }
  static readonly className: string = 'DataDeletionProperty';
}

export class DefinerProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'definerproperty'; }
  static readonly className: string = 'DefinerProperty';
}

export class DistKeyProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'distkeyproperty'; }
  static readonly className: string = 'DistKeyProperty';
}

export class DistributedByProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'kind': true, 'buckets': false, 'order': false };
  override get key(): string { return 'distributedbyproperty'; }
  static readonly className: string = 'DistributedByProperty';
}

export class DistStyleProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'diststyleproperty'; }
  static readonly className: string = 'DistStyleProperty';
}

export class DuplicateKeyProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'duplicatekeyproperty'; }
  static readonly className: string = 'DuplicateKeyProperty';
}

export class EngineProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'engineproperty'; }
  static readonly className: string = 'EngineProperty';
}

export class HeapProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'heapproperty'; }
  static readonly className: string = 'HeapProperty';
}

export class ToTableProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'totableproperty'; }
  static readonly className: string = 'ToTableProperty';
}

export class ExecuteAsProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'executeasproperty'; }
  static readonly className: string = 'ExecuteAsProperty';
}

export class ExternalProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'externalproperty'; }
  static readonly className: string = 'ExternalProperty';
}

export class FallbackProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'no': true, 'protection': false };
  override get key(): string { return 'fallbackproperty'; }
  static readonly className: string = 'FallbackProperty';
}

export class FileFormatProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': false, 'hive_format': false };
  override get key(): string { return 'fileformatproperty'; }
  static readonly className: string = 'FileFormatProperty';
}

export class CredentialsProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'credentialsproperty'; }
  static readonly className: string = 'CredentialsProperty';
}

export class FreespaceProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'percent': false };
  override get key(): string { return 'freespaceproperty'; }
  static readonly className: string = 'FreespaceProperty';
}

export class GlobalProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'globalproperty'; }
  static readonly className: string = 'GlobalProperty';
}

export class IcebergProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'icebergproperty'; }
  static readonly className: string = 'IcebergProperty';
}

export class InheritsProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'inheritsproperty'; }
  static readonly className: string = 'InheritsProperty';
}

export class InputModelProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'inputmodelproperty'; }
  static readonly className: string = 'InputModelProperty';
}

export class OutputModelProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'outputmodelproperty'; }
  static readonly className: string = 'OutputModelProperty';
}

export class IsolatedLoadingProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'no': false, 'concurrent': false, 'target': false };
  override get key(): string { return 'isolatedloadingproperty'; }
  static readonly className: string = 'IsolatedLoadingProperty';
}

export class JournalProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'no': false, 'dual': false, 'before': false, 'local': false, 'after': false };
  override get key(): string { return 'journalproperty'; }
  static readonly className: string = 'JournalProperty';
}

export class LanguageProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'languageproperty'; }
  static readonly className: string = 'LanguageProperty';
}

export class EnviromentProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'enviromentproperty'; }
  static readonly className: string = 'EnviromentProperty';
}

export class ClusteredByProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'sorted_by': false, 'buckets': true };
  override get key(): string { return 'clusteredbyproperty'; }
  static readonly className: string = 'ClusteredByProperty';
}

export class DictProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': true, 'settings': false };
  override get key(): string { return 'dictproperty'; }
  static readonly className: string = 'DictProperty';
}

export class DictSubProperty extends Property {
  override get key(): string { return 'dictsubproperty'; }
  static readonly className: string = 'DictSubProperty';
}

export class DictRange extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'min': true, 'max': true };
  override get key(): string { return 'dictrange'; }
  static readonly className: string = 'DictRange';
}

export class DynamicProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'dynamicproperty'; }
  static readonly className: string = 'DynamicProperty';
}

export class OnCluster extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'oncluster'; }
  static readonly className: string = 'OnCluster';
}

export class EmptyProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'emptyproperty'; }
  static readonly className: string = 'EmptyProperty';
}

export class LikeProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  override get key(): string { return 'likeproperty'; }
  static readonly className: string = 'LikeProperty';
}

export class LocationProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'locationproperty'; }
  static readonly className: string = 'LocationProperty';
}

export class LockProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'lockproperty'; }
  static readonly className: string = 'LockProperty';
}

export class LockingProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'kind': true, 'for_or_in': false, 'lock_type': true, 'override': false };
  override get key(): string { return 'lockingproperty'; }
  static readonly className: string = 'LockingProperty';
}

export class LogProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'no': true };
  override get key(): string { return 'logproperty'; }
  static readonly className: string = 'LogProperty';
}

export class MaterializedProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'materializedproperty'; }
  static readonly className: string = 'MaterializedProperty';
}

export class MergeBlockRatioProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'no': false, 'default': false, 'percent': false };
  override get key(): string { return 'mergeblockratioproperty'; }
  static readonly className: string = 'MergeBlockRatioProperty';
}

export class NoPrimaryIndexProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'noprimaryindexproperty'; }
  static readonly className: string = 'NoPrimaryIndexProperty';
}

export class OnProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'onproperty'; }
  static readonly className: string = 'OnProperty';
}

export class OnCommitProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'delete': false };
  override get key(): string { return 'oncommitproperty'; }
  static readonly className: string = 'OnCommitProperty';
}

export class PartitionedByProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'partitionedbyproperty'; }
  static readonly className: string = 'PartitionedByProperty';
}

export class PartitionedByBucket extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'partitionedbybucket'; }
  static readonly className: string = 'PartitionedByBucket';
}

export class PartitionByTruncate extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'partitionbytruncate'; }
  static readonly className: string = 'PartitionByTruncate';
}

export class PartitionByRangeProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'partition_expressions': true, 'create_expressions': true };
  override get key(): string { return 'partitionbyrangeproperty'; }
  static readonly className: string = 'PartitionByRangeProperty';
}

export class PartitionByRangePropertyDynamic extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'start': true, 'end': true, 'every': true };
  get key(): string { return 'partitionbyrangepropertydynamic'; }
  static readonly className: string = 'PartitionByRangePropertyDynamic';
}

export class RollupProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'rollupproperty'; }
  static readonly className: string = 'RollupProperty';
}

export class RollupIndex extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'from_index': false, 'properties': false };
  get key(): string { return 'rollupindex'; }
  static readonly className: string = 'RollupIndex';
}

export class PartitionByListProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'partition_expressions': true, 'create_expressions': true };
  override get key(): string { return 'partitionbylistproperty'; }
  static readonly className: string = 'PartitionByListProperty';
}

export class PartitionList extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  get key(): string { return 'partitionlist'; }
  static readonly className: string = 'PartitionList';
}

export class RefreshTriggerProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'method': false, 'kind': false, 'every': false, 'unit': false, 'starts': false };
  override get key(): string { return 'refreshtriggerproperty'; }
  static readonly className: string = 'RefreshTriggerProperty';
}

export class UniqueKeyProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'uniquekeyproperty'; }
  static readonly className: string = 'UniqueKeyProperty';
}

export class PartitionBoundSpec extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expression': false, 'from_expressions': false, 'to_expressions': false };
  get key(): string { return 'partitionboundspec'; }
  static readonly className: string = 'PartitionBoundSpec';
}

export class PartitionedOfProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'partitionedofproperty'; }
  static readonly className: string = 'PartitionedOfProperty';
}

export class StreamingTableProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'streamingtableproperty'; }
  static readonly className: string = 'StreamingTableProperty';
}

export class RemoteWithConnectionModelProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'remotewithconnectionmodelproperty'; }
  static readonly className: string = 'RemoteWithConnectionModelProperty';
}

export class ReturnsProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'is_table': false, 'table': false, 'null': false };
  override get key(): string { return 'returnsproperty'; }
  static readonly className: string = 'ReturnsProperty';
}

export class StrictProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'strictproperty'; }
  static readonly className: string = 'StrictProperty';
}

export class RowFormatProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'rowformatproperty'; }
  static readonly className: string = 'RowFormatProperty';
}

export class RowFormatDelimitedProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'fields': false, 'escaped': false, 'collection_items': false, 'map_keys': false, 'lines': false, 'null': false, 'serde': false };
  override get key(): string { return 'rowformatdelimitedproperty'; }
  static readonly className: string = 'RowFormatDelimitedProperty';
}

export class RowFormatSerdeProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'serde_properties': false };
  override get key(): string { return 'rowformatserdeproperty'; }
  static readonly className: string = 'RowFormatSerdeProperty';
}

export class QueryTransform extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'command_script': true, 'schema': false, 'row_format_before': false, 'record_writer': false, 'row_format_after': false, 'record_reader': false };
  get key(): string { return 'querytransform'; }
  static readonly className: string = 'QueryTransform';
}

export class SampleProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'sampleproperty'; }
  static readonly className: string = 'SampleProperty';
}

export class SecurityProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'securityproperty'; }
  static readonly className: string = 'SecurityProperty';
}

export class SchemaCommentProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'schemacommentproperty'; }
  static readonly className: string = 'SchemaCommentProperty';
}

export class SemanticView extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'metrics': false, 'dimensions': false, 'facts': false, 'where': false };
  get key(): string { return 'semanticview'; }
  static readonly className: string = 'SemanticView';
}

export class SerdeProperties extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'with_': false };
  override get key(): string { return 'serdeproperties'; }
  static readonly className: string = 'SerdeProperties';
}

export class SetProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'multi': true };
  override get key(): string { return 'setproperty'; }
  static readonly className: string = 'SetProperty';
}

export class SharingProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'sharingproperty'; }
  static readonly className: string = 'SharingProperty';
}

export class SetConfigProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'setconfigproperty'; }
  static readonly className: string = 'SetConfigProperty';
}

export class SettingsProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'settingsproperty'; }
  static readonly className: string = 'SettingsProperty';
}

export class SortKeyProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'compound': false };
  override get key(): string { return 'sortkeyproperty'; }
  static readonly className: string = 'SortKeyProperty';
}

export class SqlReadWriteProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'sqlreadwriteproperty'; }
  static readonly className: string = 'SqlReadWriteProperty';
}

export class SqlSecurityProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'sqlsecurityproperty'; }
  static readonly className: string = 'SqlSecurityProperty';
}

export class StabilityProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'stabilityproperty'; }
  static readonly className: string = 'StabilityProperty';
}

export class StorageHandlerProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'storagehandlerproperty'; }
  static readonly className: string = 'StorageHandlerProperty';
}

export class TemporaryProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'temporaryproperty'; }
  static readonly className: string = 'TemporaryProperty';
}

export class SecureProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'secureproperty'; }
  static readonly className: string = 'SecureProperty';
}

// Also extends: Property
export class Tags extends ColumnConstraintKind {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'tags'; }
  static readonly className: string = 'Tags';
}

export class TransformModelProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'transformmodelproperty'; }
  static readonly className: string = 'TransformModelProperty';
}

export class TransientProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'transientproperty'; }
  static readonly className: string = 'TransientProperty';
}

export class UnloggedProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'unloggedproperty'; }
  static readonly className: string = 'UnloggedProperty';
}

export class UsingTemplateProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'usingtemplateproperty'; }
  static readonly className: string = 'UsingTemplateProperty';
}

export class ViewAttributeProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'viewattributeproperty'; }
  static readonly className: string = 'ViewAttributeProperty';
}

export class VolatileProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'volatileproperty'; }
  static readonly className: string = 'VolatileProperty';
}

export class WithDataProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'no': true, 'statistics': false };
  override get key(): string { return 'withdataproperty'; }
  static readonly className: string = 'WithDataProperty';
}

export class WithJournalTableProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'withjournaltableproperty'; }
  static readonly className: string = 'WithJournalTableProperty';
}

export class WithSchemaBindingProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'withschemabindingproperty'; }
  static readonly className: string = 'WithSchemaBindingProperty';
}

export class WithSystemVersioningProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'on': false, 'this': false, 'data_consistency': false, 'retention_period': false, 'with_': true };
  override get key(): string { return 'withsystemversioningproperty'; }
  static readonly className: string = 'WithSystemVersioningProperty';
}

export class WithProcedureOptions extends Property {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'withprocedureoptions'; }
  static readonly className: string = 'WithProcedureOptions';
}

export class EncodeProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'properties': false, 'key': false };
  override get key(): string { return 'encodeproperty'; }
  static readonly className: string = 'EncodeProperty';
}

export class IncludeProperty extends Property {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alias': false, 'column_def': false };
  override get key(): string { return 'includeproperty'; }
  static readonly className: string = 'IncludeProperty';
}

export class ForceProperty extends Property {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'forceproperty'; }
  static readonly className: string = 'ForceProperty';
}

export class Properties extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'properties'; }
  static readonly className: string = 'Properties';
}

export class Qualify extends Expression {
  get key(): string { return 'qualify'; }
  static readonly className: string = 'Qualify';
}

export class InputOutputFormat extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'input_format': false, 'output_format': false };
  get key(): string { return 'inputoutputformat'; }
  static readonly className: string = 'InputOutputFormat';
}

export class Return extends Expression {
  get key(): string { return 'return'; }
  static readonly className: string = 'Return';
}

export class Reference extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'options': false };
  get key(): string { return 'reference'; }
  static readonly className: string = 'Reference';
}

export class Tuple extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  get key(): string { return 'tuple'; }
  static readonly className: string = 'Tuple';
}

export class QueryOption extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  get key(): string { return 'queryoption'; }
  static readonly className: string = 'QueryOption';
}

export class WithTableHint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'withtablehint'; }
  static readonly className: string = 'WithTableHint';
}

export class IndexTableHint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'target': false };
  get key(): string { return 'indextablehint'; }
  static readonly className: string = 'IndexTableHint';
}

export class HistoricalData extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': true, 'expression': true };
  get key(): string { return 'historicaldata'; }
  static readonly className: string = 'HistoricalData';
}

export class Put extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'target': true, 'properties': false };
  get key(): string { return 'put'; }
  static readonly className: string = 'Put';
}

export class Get extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'target': true, 'properties': false };
  get key(): string { return 'get'; }
  static readonly className: string = 'Get';
}

export class Table extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'alias': false, 'db': false, 'catalog': false, 'laterals': false, 'joins': false, 'pivots': false, 'hints': false, 'system_time': false, 'version': false, 'format': false, 'pattern': false, 'ordinality': false, 'when': false, 'only': false, 'partition': false, 'changes': false, 'rows_from': false, 'sample': false, 'indexed': false };
  get key(): string { return 'table'; }
  static readonly className: string = 'Table';
  get name(): string { return this.text('this'); }
  get db(): string { return this.text('db'); }
  get catalog(): string { return this.text('catalog'); }
}

export class SetOperation extends Query {
  static readonly argTypes: Record<string, boolean> = { 'with_': false, 'this': true, 'expression': true, 'distinct': false, 'by_name': false, 'side': false, 'kind': false, 'on': false, 'match': false, 'laterals': false, 'joins': false, 'connect': false, 'pivots': false, 'prewhere': false, 'where': false, 'group': false, 'having': false, 'qualify': false, 'windows': false, 'distribute': false, 'sort': false, 'cluster': false, 'order': false, 'limit': false, 'offset': false, 'locks': false, 'sample': false, 'settings': false, 'format': false, 'options': false };
  override get key(): string { return 'setoperation'; }
  static readonly className: string = 'SetOperation';
}

export class Union extends SetOperation {
  override get key(): string { return 'union'; }
  static readonly className: string = 'Union';
}

export class Except extends SetOperation {
  override get key(): string { return 'except'; }
  static readonly className: string = 'Except';
}

export class Intersect extends SetOperation {
  override get key(): string { return 'intersect'; }
  static readonly className: string = 'Intersect';
}

export class Update extends DML {
  static readonly argTypes: Record<string, boolean> = { 'with_': false, 'this': false, 'expressions': false, 'from_': false, 'where': false, 'returning': false, 'order': false, 'limit': false, 'options': false };
  override get key(): string { return 'update'; }
  static readonly className: string = 'Update';
}

export class Values extends UDTF {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'alias': false, 'order': false, 'limit': false, 'offset': false };
  override get key(): string { return 'values'; }
  static readonly className: string = 'Values';
}

export class Var extends Expression {
  get key(): string { return 'var'; }
  static readonly className: string = 'Var';
  get name(): string { return this.text('this'); }
}

export class Version extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': true, 'expression': false };
  get key(): string { return 'version'; }
  static readonly className: string = 'Version';
}

export class Schema extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': false };
  get key(): string { return 'schema'; }
  static readonly className: string = 'Schema';
}

export class Lock extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'update': true, 'expressions': false, 'wait': false, 'key': false };
  get key(): string { return 'lock'; }
  static readonly className: string = 'Lock';
}

export class Select extends Query {
  static readonly argTypes: Record<string, boolean> = { 'with_': false, 'kind': false, 'expressions': false, 'hint': false, 'distinct': false, 'into': false, 'from_': false, 'operation_modifiers': false, 'match': false, 'laterals': false, 'joins': false, 'connect': false, 'pivots': false, 'prewhere': false, 'where': false, 'group': false, 'having': false, 'qualify': false, 'windows': false, 'distribute': false, 'sort': false, 'cluster': false, 'order': false, 'limit': false, 'offset': false, 'locks': false, 'sample': false, 'settings': false, 'format': false, 'options': false };
  override get key(): string { return 'select'; }
  static readonly className: string = 'Select';
  get selects(): Expression[] {
    return this.expressions;
  }
  get namedSelects(): string[] {
    return this.expressions.map(expr => expr.outputName);
  }
  get from_(): Expression | undefined {
    const from = this.args['from_'];
    return from instanceof Expression ? from : undefined;
  }
  get where_(): Expression | undefined {
    const where = this.args['where'];
    return where instanceof Expression ? where : undefined;
  }
  from(expression: string | Expression, copy = true): Select {
    return _applyBuilder(expression, this, 'from_', { copy, into: From, prefix: 'FROM' }) as Select;
  }
  select(...expressions: (string | Expression)[]): Select {
    return _applyListBuilder(expressions, this, 'expressions', { copy: true, append: true }) as Select;
  }
  join(expression: string | Expression, options?: { on?: string | Expression; using?: (string | Expression)[]; joinType?: string; copy?: boolean }): Select {
    const copy = options?.copy ?? true;
    let join: Expression;
    try { join = maybeParse(expression, { into: Join, prefix: 'JOIN' }); } catch { join = maybeParse(expression); }
    if (!(join instanceof Join)) { join = new Join({ this: join }); }
    if (options?.joinType) {
      const parts = options.joinType.toUpperCase().split(/\s+/);
      for (const p of parts) {
        if (p === 'LEFT' || p === 'RIGHT' || p === 'FULL') join.set('side', p);
        else if (p === 'INNER' || p === 'OUTER' || p === 'CROSS' || p === 'SEMI' || p === 'ANTI') join.set('kind', p);
        else if (p === 'NATURAL') join.set('method', p);
      }
    }
    if (options?.on) {
      join.set('on', maybeParse(options.on));
    }
    if (options?.using) {
      join.set('using', options.using.map(u => maybeParse(u)));
    }
    return _applyListBuilder([join], this, 'joins', { copy, append: true }) as Select;
  }
  groupBy(...expressions: (string | Expression)[]): Select {
    return _applyChildListBuilder(expressions, this, 'group', { copy: true, into: Group, prefix: 'GROUP BY' }) as Select;
  }
  having(...expressions: (string | Expression)[]): Select {
    return _applyConjunctionBuilder(expressions, this, 'having', { copy: true, into: Having, append: true }) as Select;
  }
  distinct(value = true): Select {
    const inst = this.copy() as Select;
    inst.set('distinct', value ? new Distinct({}) : undefined);
    return inst;
  }
  qualify(...expressions: (string | Expression)[]): Select {
    return _applyConjunctionBuilder(expressions, this, 'qualify', { copy: true, into: Qualify, append: true }) as Select;
  }
  sortBy(...expressions: (string | Expression)[]): Select {
    return _applyChildListBuilder(expressions, this, 'sort', { copy: true, into: Sort, prefix: 'SORT BY' }) as Select;
  }
  clusterBy(...expressions: (string | Expression)[]): Select {
    return _applyChildListBuilder(expressions, this, 'cluster', { copy: true, into: Cluster, prefix: 'CLUSTER BY' }) as Select;
  }
  lateral(...expressions: (string | Expression)[]): Select {
    return _applyListBuilder(expressions, this, 'laterals', { copy: true, append: true, into: Lateral, prefix: 'LATERAL VIEW' }) as Select;
  }
  window_(...expressions: (string | Expression)[]): Select {
    return _applyListBuilder(expressions, this, 'windows', { copy: true, append: true, into: Window, prefix: 'WINDOW' }) as Select;
  }
  ctas(table: string | Expression, options?: { dialect?: string; copy?: boolean }): Create {
    const inst = options?.copy !== false ? this.copy() : this;
    const tableExpr = maybeParse(table, { into: Table, dialect: options?.dialect });
    return new Create({ this: tableExpr, kind: 'TABLE', expression: inst });
  }
  lock(update = true, copy = true): Select {
    const inst = copy ? this.copy() as Select : this;
    inst.set('locks', [new Lock({ update })]);
    return inst;
  }
  hint(...hints: (string | Expression)[]): Select {
    const inst = this.copy() as Select;
    const parsed = hints.map(h => maybeParse(h));
    inst.set('hint', new Hint({ expressions: parsed }));
    return inst;
  }
}

// Also extends: Query
export class Subquery extends DerivedTable {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alias': false, 'with_': false, 'match': false, 'laterals': false, 'joins': false, 'connect': false, 'pivots': false, 'prewhere': false, 'where': false, 'group': false, 'having': false, 'qualify': false, 'windows': false, 'distribute': false, 'sort': false, 'cluster': false, 'order': false, 'limit': false, 'offset': false, 'locks': false, 'sample': false, 'settings': false, 'format': false, 'options': false };
  override get key(): string { return 'subquery'; }
  static readonly className: string = 'Subquery';
  limit(expression: string | Expression, copy = true): this {
    return _applyBuilder(expression, this, 'limit', { copy, into: Limit, prefix: 'LIMIT' }) as this;
  }
  offset(expression: string | Expression, copy = true): this {
    return _applyBuilder(expression, this, 'offset', { copy, into: Offset, prefix: 'OFFSET' }) as this;
  }
  orderBy(...expressions: (string | Expression)[]): this {
    return _applyChildListBuilder(expressions, this, 'order', { copy: true, into: Order, prefix: 'ORDER BY' }) as this;
  }
  where(...expressions: (string | Expression)[]): this {
    return _applyConjunctionBuilder(expressions, this, 'where', { copy: true, into: Where, append: true }) as this;
  }
}

export class TableSample extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'method': false, 'bucket_numerator': false, 'bucket_denominator': false, 'bucket_field': false, 'percent': false, 'rows': false, 'size': false, 'seed': false };
  get key(): string { return 'tablesample'; }
  static readonly className: string = 'TableSample';
}

export class Tag extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'prefix': false, 'postfix': false };
  get key(): string { return 'tag'; }
  static readonly className: string = 'Tag';
}

export class Pivot extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'alias': false, 'expressions': false, 'fields': false, 'unpivot': false, 'using': false, 'group': false, 'columns': false, 'include_nulls': false, 'default_on_null': false, 'into': false, 'with_': false };
  get key(): string { return 'pivot'; }
  static readonly className: string = 'Pivot';
}

export class UnpivotColumns extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  get key(): string { return 'unpivotcolumns'; }
  static readonly className: string = 'UnpivotColumns';
}

export class Window extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'partition_by': false, 'order': false, 'spec': false, 'alias': false, 'over': false, 'first': false };
  override get key(): string { return 'window'; }
  static readonly className: string = 'Window';
}

export class WindowSpec extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'kind': false, 'start': false, 'start_side': false, 'end': false, 'end_side': false, 'exclude': false };
  get key(): string { return 'windowspec'; }
  static readonly className: string = 'WindowSpec';
}

export class PreWhere extends Expression {
  get key(): string { return 'prewhere'; }
  static readonly className: string = 'PreWhere';
}

export class Where extends Expression {
  get key(): string { return 'where'; }
  static readonly className: string = 'Where';
}

export class Star extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'except_': false, 'replace': false, 'rename': false };
  get key(): string { return 'star'; }
  static readonly className: string = 'Star';
  override get isStar(): boolean { return true; }
}

export class Parameter extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'parameter'; }
  static readonly className: string = 'Parameter';
}

export class SessionParameter extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': false };
  override get key(): string { return 'sessionparameter'; }
  static readonly className: string = 'SessionParameter';
}

export class Placeholder extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'kind': false, 'widget': false, 'jdbc': false };
  override get key(): string { return 'placeholder'; }
  static readonly className: string = 'Placeholder';
}

export class Null extends Condition {
  override get key(): string { return 'null'; }
  static readonly className: string = 'Null';
}

export class Boolean extends Condition {
  override get key(): string { return 'boolean'; }
  static readonly className: string = 'Boolean';
  get value(): boolean { return !!this.args['this']; }
  static true_(): Boolean { return new Boolean({ this: true }); }
  static false_(): Boolean { return new Boolean({ this: false }); }
}

export class DataTypeParam extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  get key(): string { return 'datatypeparam'; }
  static readonly className: string = 'DataTypeParam';
}

export class DataType extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'nested': false, 'values': false, 'prefix': false, 'kind': false, 'nullable': false };
  get key(): string { return 'datatype'; }
  static readonly className: string = 'DataType';
}

export class PseudoType extends DataType {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'pseudotype'; }
  static readonly className: string = 'PseudoType';
}

export class ObjectIdentifier extends DataType {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'objectidentifier'; }
  static readonly className: string = 'ObjectIdentifier';
}

export class SubqueryPredicate extends Predicate {
  override get key(): string { return 'subquerypredicate'; }
  static readonly className: string = 'SubqueryPredicate';
}

export class All extends SubqueryPredicate {
  override get key(): string { return 'all'; }
  static readonly className: string = 'All';
}

export class Any extends SubqueryPredicate {
  override get key(): string { return 'any'; }
  static readonly className: string = 'Any';
}

export class Command extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  get key(): string { return 'command'; }
  static readonly className: string = 'Command';
}

export class Transaction extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'modes': false, 'mark': false };
  get key(): string { return 'transaction'; }
  static readonly className: string = 'Transaction';
}

export class Commit extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'chain': false, 'this': false, 'durability': false };
  get key(): string { return 'commit'; }
  static readonly className: string = 'Commit';
}

export class Rollback extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'savepoint': false, 'this': false };
  get key(): string { return 'rollback'; }
  static readonly className: string = 'Rollback';
}

export class Alter extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'kind': true, 'actions': true, 'exists': false, 'only': false, 'options': false, 'cluster': false, 'not_valid': false, 'check': false, 'cascade': false };
  get key(): string { return 'alter'; }
  static readonly className: string = 'Alter';
}

export class AlterSession extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'unset': false };
  get key(): string { return 'altersession'; }
  static readonly className: string = 'AlterSession';
}

export class Analyze extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'kind': false, 'this': false, 'options': false, 'mode': false, 'partition': false, 'expression': false, 'properties': false };
  get key(): string { return 'analyze'; }
  static readonly className: string = 'Analyze';
}

export class AnalyzeStatistics extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'kind': true, 'option': false, 'this': false, 'expressions': false };
  get key(): string { return 'analyzestatistics'; }
  static readonly className: string = 'AnalyzeStatistics';
}

export class AnalyzeHistogram extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'expression': false, 'update_options': false };
  get key(): string { return 'analyzehistogram'; }
  static readonly className: string = 'AnalyzeHistogram';
}

export class AnalyzeSample extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'kind': true, 'sample': true };
  get key(): string { return 'analyzesample'; }
  static readonly className: string = 'AnalyzeSample';
}

export class AnalyzeListChainedRows extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expression': false };
  get key(): string { return 'analyzelistchainedrows'; }
  static readonly className: string = 'AnalyzeListChainedRows';
}

export class AnalyzeDelete extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'kind': false };
  get key(): string { return 'analyzedelete'; }
  static readonly className: string = 'AnalyzeDelete';
}

export class AnalyzeWith extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'analyzewith'; }
  static readonly className: string = 'AnalyzeWith';
}

export class AnalyzeValidate extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'kind': true, 'this': false, 'expression': false };
  get key(): string { return 'analyzevalidate'; }
  static readonly className: string = 'AnalyzeValidate';
}

export class AnalyzeColumns extends Expression {
  get key(): string { return 'analyzecolumns'; }
  static readonly className: string = 'AnalyzeColumns';
}

export class UsingData extends Expression {
  get key(): string { return 'usingdata'; }
  static readonly className: string = 'UsingData';
}

export class AddConstraint extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'addconstraint'; }
  static readonly className: string = 'AddConstraint';
}

export class AddPartition extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'exists': false, 'location': false };
  get key(): string { return 'addpartition'; }
  static readonly className: string = 'AddPartition';
}

export class AttachOption extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  get key(): string { return 'attachoption'; }
  static readonly className: string = 'AttachOption';
}

export class DropPartition extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'exists': false };
  get key(): string { return 'droppartition'; }
  static readonly className: string = 'DropPartition';
}

export class ReplacePartition extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expression': true, 'source': true };
  get key(): string { return 'replacepartition'; }
  static readonly className: string = 'ReplacePartition';
}

export class Binary extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'binary'; }
  static readonly className: string = 'Binary';
  get left(): Expression | undefined {
    const val = this.args['this'];
    return val instanceof Expression ? val : undefined;
  }
  get right(): Expression | undefined {
    const val = this.args['expression'];
    return val instanceof Expression ? val : undefined;
  }
}

export class Add extends Binary {
  override get key(): string { return 'add'; }
  static readonly className: string = 'Add';
}

export class Connector extends Binary {
  override get key(): string { return 'connector'; }
  static readonly className: string = 'Connector';
}

export class BitwiseAnd extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'padside': false };
  override get key(): string { return 'bitwiseand'; }
  static readonly className: string = 'BitwiseAnd';
}

export class BitwiseLeftShift extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'requires_int128': false };
  override get key(): string { return 'bitwiseleftshift'; }
  static readonly className: string = 'BitwiseLeftShift';
}

export class BitwiseOr extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'padside': false };
  override get key(): string { return 'bitwiseor'; }
  static readonly className: string = 'BitwiseOr';
}

export class BitwiseRightShift extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'requires_int128': false };
  override get key(): string { return 'bitwiserightshift'; }
  static readonly className: string = 'BitwiseRightShift';
}

export class BitwiseXor extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'padside': false };
  override get key(): string { return 'bitwisexor'; }
  static readonly className: string = 'BitwiseXor';
}

export class Div extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'typed': false, 'safe': false };
  override get key(): string { return 'div'; }
  static readonly className: string = 'Div';
}

export class Overlaps extends Binary {
  override get key(): string { return 'overlaps'; }
  static readonly className: string = 'Overlaps';
}

export class ExtendsLeft extends Binary {
  override get key(): string { return 'extendsleft'; }
  static readonly className: string = 'ExtendsLeft';
}

export class ExtendsRight extends Binary {
  override get key(): string { return 'extendsright'; }
  static readonly className: string = 'ExtendsRight';
}

export class Dot extends Binary {
  override get key(): string { return 'dot'; }
  static readonly className: string = 'Dot';
}

export class DPipe extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'safe': false };
  override get key(): string { return 'dpipe'; }
  static readonly className: string = 'DPipe';
}

// Also extends: Predicate
export class EQ extends Binary {
  override get key(): string { return 'eq'; }
  static readonly className: string = 'EQ';
}

// Also extends: Predicate
export class NullSafeEQ extends Binary {
  override get key(): string { return 'nullsafeeq'; }
  static readonly className: string = 'NullSafeEQ';
}

// Also extends: Predicate
export class NullSafeNEQ extends Binary {
  override get key(): string { return 'nullsafeneq'; }
  static readonly className: string = 'NullSafeNEQ';
}

export class PropertyEQ extends Binary {
  override get key(): string { return 'propertyeq'; }
  static readonly className: string = 'PropertyEQ';
}

export class Distance extends Binary {
  override get key(): string { return 'distance'; }
  static readonly className: string = 'Distance';
}

export class Escape extends Binary {
  override get key(): string { return 'escape'; }
  static readonly className: string = 'Escape';
}

// Also extends: Predicate
export class Glob extends Binary {
  override get key(): string { return 'glob'; }
  static readonly className: string = 'Glob';
}

// Also extends: Predicate
export class GT extends Binary {
  override get key(): string { return 'gt'; }
  static readonly className: string = 'GT';
}

// Also extends: Predicate
export class GTE extends Binary {
  override get key(): string { return 'gte'; }
  static readonly className: string = 'GTE';
}

// Also extends: Predicate
export class ILike extends Binary {
  override get key(): string { return 'ilike'; }
  static readonly className: string = 'ILike';
}

export class IntDiv extends Binary {
  override get key(): string { return 'intdiv'; }
  static readonly className: string = 'IntDiv';
}

// Also extends: Predicate
export class Is extends Binary {
  override get key(): string { return 'is'; }
  static readonly className: string = 'Is';
}

export class Kwarg extends Binary {
  override get key(): string { return 'kwarg'; }
  static readonly className: string = 'Kwarg';
}

// Also extends: Predicate
export class Like extends Binary {
  override get key(): string { return 'like'; }
  static readonly className: string = 'Like';
}

// Also extends: Predicate
export class Match extends Binary {
  override get key(): string { return 'match'; }
  static readonly className: string = 'Match';
}

// Also extends: Predicate
export class LT extends Binary {
  override get key(): string { return 'lt'; }
  static readonly className: string = 'LT';
}

// Also extends: Predicate
export class LTE extends Binary {
  override get key(): string { return 'lte'; }
  static readonly className: string = 'LTE';
}

export class Mod extends Binary {
  override get key(): string { return 'mod'; }
  static readonly className: string = 'Mod';
}

export class Mul extends Binary {
  override get key(): string { return 'mul'; }
  static readonly className: string = 'Mul';
}

// Also extends: Predicate
export class NEQ extends Binary {
  override get key(): string { return 'neq'; }
  static readonly className: string = 'NEQ';
}

export class Operator extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'operator': true, 'expression': true };
  override get key(): string { return 'operator'; }
  static readonly className: string = 'Operator';
}

// Also extends: Predicate
export class SimilarTo extends Binary {
  override get key(): string { return 'similarto'; }
  static readonly className: string = 'SimilarTo';
}

export class Sub extends Binary {
  override get key(): string { return 'sub'; }
  static readonly className: string = 'Sub';
}

export class Adjacent extends Binary {
  override get key(): string { return 'adjacent'; }
  static readonly className: string = 'Adjacent';
}

export class Unary extends Condition {
  override get key(): string { return 'unary'; }
  static readonly className: string = 'Unary';
}

export class BitwiseNot extends Unary {
  override get key(): string { return 'bitwisenot'; }
  static readonly className: string = 'BitwiseNot';
}

export class Not extends Unary {
  override get key(): string { return 'not'; }
  static readonly className: string = 'Not';
}

export class Paren extends Unary {
  override get key(): string { return 'paren'; }
  static readonly className: string = 'Paren';
}

export class Neg extends Unary {
  override get key(): string { return 'neg'; }
  static readonly className: string = 'Neg';
}

export class Alias extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alias': false };
  get key(): string { return 'alias'; }
  static readonly className: string = 'Alias';
}

export class PivotAlias extends Alias {
  override get key(): string { return 'pivotalias'; }
  static readonly className: string = 'PivotAlias';
}

export class PivotAny extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  get key(): string { return 'pivotany'; }
  static readonly className: string = 'PivotAny';
}

export class Aliases extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  get key(): string { return 'aliases'; }
  static readonly className: string = 'Aliases';
}

export class AtIndex extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'atindex'; }
  static readonly className: string = 'AtIndex';
}

export class AtTimeZone extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'zone': true };
  get key(): string { return 'attimezone'; }
  static readonly className: string = 'AtTimeZone';
}

export class FromTimeZone extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'zone': true };
  get key(): string { return 'fromtimezone'; }
  static readonly className: string = 'FromTimeZone';
}

export class FormatPhrase extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': true };
  get key(): string { return 'formatphrase'; }
  static readonly className: string = 'FormatPhrase';
}

export class Between extends Predicate {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'low': true, 'high': true, 'symmetric': false };
  override get key(): string { return 'between'; }
  static readonly className: string = 'Between';
}

export class Bracket extends Condition {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'offset': false, 'safe': false, 'returns_list_for_maps': false };
  override get key(): string { return 'bracket'; }
  static readonly className: string = 'Bracket';
}

export class Distinct extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'on': false };
  get key(): string { return 'distinct'; }
  static readonly className: string = 'Distinct';
}

export class In extends Predicate {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'query': false, 'unnest': false, 'field': false, 'is_global': false };
  override get key(): string { return 'in'; }
  static readonly className: string = 'In';
}

export class ForIn extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'forin'; }
  static readonly className: string = 'ForIn';
}

export class TimeUnit extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'unit': false };
  get key(): string { return 'timeunit'; }
  static readonly className: string = 'TimeUnit';
}

export class IntervalOp extends TimeUnit {
  static readonly argTypes: Record<string, boolean> = { 'unit': false, 'expression': true };
  override get key(): string { return 'intervalop'; }
  static readonly className: string = 'IntervalOp';
}

export class IntervalSpan extends DataType {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'intervalspan'; }
  static readonly className: string = 'IntervalSpan';
}

export class Interval extends TimeUnit {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'unit': false };
  override get key(): string { return 'interval'; }
  static readonly className: string = 'Interval';
}

export class IgnoreNulls extends Expression {
  get key(): string { return 'ignorenulls'; }
  static readonly className: string = 'IgnoreNulls';
}

export class RespectNulls extends Expression {
  get key(): string { return 'respectnulls'; }
  static readonly className: string = 'RespectNulls';
}

export class HavingMax extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'max': true };
  get key(): string { return 'havingmax'; }
  static readonly className: string = 'HavingMax';
}

export class Func extends Condition {
  override get key(): string { return 'func'; }
  static readonly className: string = 'Func';
  static readonly sqlNames: readonly string[] | undefined = undefined;
  get name(): string {
    const ctor = this.constructor as typeof Func;
    const first = ctor.sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class SafeFunc extends Func {
  override get key(): string { return 'safefunc'; }
  static readonly className: string = 'SafeFunc';
}

export class Typeof extends Func {
  override get key(): string { return 'typeof'; }
  static readonly className: string = 'Typeof';
}

export class Acos extends Func {
  override get key(): string { return 'acos'; }
  static readonly className: string = 'Acos';
}

export class Acosh extends Func {
  override get key(): string { return 'acosh'; }
  static readonly className: string = 'Acosh';
}

export class Asin extends Func {
  override get key(): string { return 'asin'; }
  static readonly className: string = 'Asin';
}

export class Asinh extends Func {
  override get key(): string { return 'asinh'; }
  static readonly className: string = 'Asinh';
}

export class Atan extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'atan'; }
  static readonly className: string = 'Atan';
}

export class Atanh extends Func {
  override get key(): string { return 'atanh'; }
  static readonly className: string = 'Atanh';
}

export class Atan2 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'atan2'; }
  static readonly className: string = 'Atan2';
}

export class Cot extends Func {
  override get key(): string { return 'cot'; }
  static readonly className: string = 'Cot';
}

export class Coth extends Func {
  override get key(): string { return 'coth'; }
  static readonly className: string = 'Coth';
}

export class Cos extends Func {
  override get key(): string { return 'cos'; }
  static readonly className: string = 'Cos';
}

export class Csc extends Func {
  override get key(): string { return 'csc'; }
  static readonly className: string = 'Csc';
}

export class Csch extends Func {
  override get key(): string { return 'csch'; }
  static readonly className: string = 'Csch';
}

export class Sec extends Func {
  override get key(): string { return 'sec'; }
  static readonly className: string = 'Sec';
}

export class Sech extends Func {
  override get key(): string { return 'sech'; }
  static readonly className: string = 'Sech';
}

export class Sin extends Func {
  override get key(): string { return 'sin'; }
  static readonly className: string = 'Sin';
}

export class Sinh extends Func {
  override get key(): string { return 'sinh'; }
  static readonly className: string = 'Sinh';
}

export class Tan extends Func {
  override get key(): string { return 'tan'; }
  static readonly className: string = 'Tan';
}

export class Tanh extends Func {
  override get key(): string { return 'tanh'; }
  static readonly className: string = 'Tanh';
}

export class Degrees extends Func {
  override get key(): string { return 'degrees'; }
  static readonly className: string = 'Degrees';
}

export class Cosh extends Func {
  override get key(): string { return 'cosh'; }
  static readonly className: string = 'Cosh';
}

export class CosineDistance extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'cosinedistance'; }
  static readonly className: string = 'CosineDistance';
}

export class DotProduct extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'dotproduct'; }
  static readonly className: string = 'DotProduct';
}

export class EuclideanDistance extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'euclideandistance'; }
  static readonly className: string = 'EuclideanDistance';
}

export class ManhattanDistance extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'manhattandistance'; }
  static readonly className: string = 'ManhattanDistance';
}

export class JarowinklerSimilarity extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'jarowinklersimilarity'; }
  static readonly className: string = 'JarowinklerSimilarity';
}

export class AggFunc extends Func {
  override get key(): string { return 'aggfunc'; }
  static readonly className: string = 'AggFunc';
}

export class BitwiseAndAgg extends AggFunc {
  override get key(): string { return 'bitwiseandagg'; }
  static readonly className: string = 'BitwiseAndAgg';
}

export class BitwiseOrAgg extends AggFunc {
  override get key(): string { return 'bitwiseoragg'; }
  static readonly className: string = 'BitwiseOrAgg';
}

export class BitwiseXorAgg extends AggFunc {
  override get key(): string { return 'bitwisexoragg'; }
  static readonly className: string = 'BitwiseXorAgg';
}

export class BoolxorAgg extends AggFunc {
  override get key(): string { return 'boolxoragg'; }
  static readonly className: string = 'BoolxorAgg';
}

export class BitwiseCount extends Func {
  override get key(): string { return 'bitwisecount'; }
  static readonly className: string = 'BitwiseCount';
}

export class BitmapBucketNumber extends Func {
  override get key(): string { return 'bitmapbucketnumber'; }
  static readonly className: string = 'BitmapBucketNumber';
}

export class BitmapCount extends Func {
  override get key(): string { return 'bitmapcount'; }
  static readonly className: string = 'BitmapCount';
}

export class BitmapBitPosition extends Func {
  override get key(): string { return 'bitmapbitposition'; }
  static readonly className: string = 'BitmapBitPosition';
}

export class BitmapConstructAgg extends AggFunc {
  override get key(): string { return 'bitmapconstructagg'; }
  static readonly className: string = 'BitmapConstructAgg';
}

export class BitmapOrAgg extends AggFunc {
  override get key(): string { return 'bitmaporagg'; }
  static readonly className: string = 'BitmapOrAgg';
}

export class ByteLength extends Func {
  override get key(): string { return 'bytelength'; }
  static readonly className: string = 'ByteLength';
}

export class Boolnot extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'round_input': false };
  override get key(): string { return 'boolnot'; }
  static readonly className: string = 'Boolnot';
}

export class Booland extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'round_input': false };
  override get key(): string { return 'booland'; }
  static readonly className: string = 'Booland';
}

export class Boolor extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'round_input': false };
  override get key(): string { return 'boolor'; }
  static readonly className: string = 'Boolor';
}

export class JSONBool extends Func {
  override get key(): string { return 'jsonbool'; }
  static readonly className: string = 'JSONBool';
}

export class ArrayRemove extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'null_propagation': false };
  override get key(): string { return 'arrayremove'; }
  static readonly className: string = 'ArrayRemove';
}

export class ParameterizedAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'params': true };
  override get key(): string { return 'parameterizedagg'; }
  static readonly className: string = 'ParameterizedAgg';
}

export class Abs extends Func {
  override get key(): string { return 'abs'; }
  static readonly className: string = 'Abs';
}

export class ArgMax extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'count': false };
  static readonly sqlNames: readonly string[] = ['ARG_MAX', 'ARGMAX', 'MAX_BY'];
  override get key(): string { return 'argmax'; }
  static readonly className: string = 'ArgMax';
}

export class ArgMin extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'count': false };
  static readonly sqlNames: readonly string[] = ['ARG_MIN', 'ARGMIN', 'MIN_BY'];
  override get key(): string { return 'argmin'; }
  static readonly className: string = 'ArgMin';
}

export class ApproxTopK extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'counters': false };
  override get key(): string { return 'approxtopk'; }
  static readonly className: string = 'ApproxTopK';
}

export class ApproxTopKAccumulate extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'approxtopkaccumulate'; }
  static readonly className: string = 'ApproxTopKAccumulate';
}

export class ApproxTopKCombine extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'approxtopkcombine'; }
  static readonly className: string = 'ApproxTopKCombine';
}

export class ApproxTopKEstimate extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'approxtopkestimate'; }
  static readonly className: string = 'ApproxTopKEstimate';
}

export class ApproxTopSum extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'count': true };
  override get key(): string { return 'approxtopsum'; }
  static readonly className: string = 'ApproxTopSum';
}

export class ApproxQuantiles extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'approxquantiles'; }
  static readonly className: string = 'ApproxQuantiles';
}

export class ApproxPercentileCombine extends AggFunc {
  override get key(): string { return 'approxpercentilecombine'; }
  static readonly className: string = 'ApproxPercentileCombine';
}

export class Minhash extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'minhash'; }
  static readonly className: string = 'Minhash';
}

export class MinhashCombine extends AggFunc {
  override get key(): string { return 'minhashcombine'; }
  static readonly className: string = 'MinhashCombine';
}

export class ApproximateSimilarity extends AggFunc {
  static readonly sqlNames: readonly string[] = ['APPROXIMATE_SIMILARITY', 'APPROXIMATE_JACCARD_INDEX'];
  override get key(): string { return 'approximatesimilarity'; }
  static readonly className: string = 'ApproximateSimilarity';
}

export class FarmFingerprint extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['FARM_FINGERPRINT', 'FARMFINGERPRINT64'];
  override get key(): string { return 'farmfingerprint'; }
  static readonly className: string = 'FarmFingerprint';
}

export class Flatten extends Func {
  override get key(): string { return 'flatten'; }
  static readonly className: string = 'Flatten';
}

export class Float64 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'float64'; }
  static readonly className: string = 'Float64';
}

export class Transform extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'transform'; }
  static readonly className: string = 'Transform';
}

export class Translate extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'from_': true, 'to': true };
  override get key(): string { return 'translate'; }
  static readonly className: string = 'Translate';
}

export class Grouping extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'grouping'; }
  static readonly className: string = 'Grouping';
}

export class GroupingId extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'groupingid'; }
  static readonly className: string = 'GroupingId';
}

export class Anonymous extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'anonymous'; }
  static readonly className: string = 'Anonymous';
  override get name(): string { return this.text('this'); }
}

export class AnonymousAggFunc extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'anonymousaggfunc'; }
  static readonly className: string = 'AnonymousAggFunc';
}

export class CombinedAggFunc extends AnonymousAggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  override get key(): string { return 'combinedaggfunc'; }
  static readonly className: string = 'CombinedAggFunc';
}

export class CombinedParameterizedAgg extends ParameterizedAgg {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'params': true };
  override get key(): string { return 'combinedparameterizedagg'; }
  static readonly className: string = 'CombinedParameterizedAgg';
}

export class HashAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'hashagg'; }
  static readonly className: string = 'HashAgg';
}

export class Hll extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'hll'; }
  static readonly className: string = 'Hll';
}

export class ApproxDistinct extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'accuracy': false };
  static readonly sqlNames: readonly string[] = ['APPROX_DISTINCT', 'APPROX_COUNT_DISTINCT'];
  override get key(): string { return 'approxdistinct'; }
  static readonly className: string = 'ApproxDistinct';
}

export class Apply extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'apply'; }
  static readonly className: string = 'Apply';
}

export class Array extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'bracket_notation': false, 'struct_name_inheritance': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'array'; }
  static readonly className: string = 'Array';
}

export class Ascii extends Func {
  override get key(): string { return 'ascii'; }
  static readonly className: string = 'Ascii';
}

export class ToArray extends Func {
  override get key(): string { return 'toarray'; }
  static readonly className: string = 'ToArray';
}

export class ToBoolean extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'safe': false };
  override get key(): string { return 'toboolean'; }
  static readonly className: string = 'ToBoolean';
}

export class List extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'list'; }
  static readonly className: string = 'List';
}

export class Pad extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'fill_pattern': false, 'is_left': true };
  override get key(): string { return 'pad'; }
  static readonly className: string = 'Pad';
}

export class ToChar extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'nlsparam': false, 'is_numeric': false };
  override get key(): string { return 'tochar'; }
  static readonly className: string = 'ToChar';
}

export class ToCodePoints extends Func {
  override get key(): string { return 'tocodepoints'; }
  static readonly className: string = 'ToCodePoints';
}

export class ToNumber extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'nlsparam': false, 'precision': false, 'scale': false, 'safe': false, 'safe_name': false };
  override get key(): string { return 'tonumber'; }
  static readonly className: string = 'ToNumber';
}

export class ToDouble extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'safe': false };
  override get key(): string { return 'todouble'; }
  static readonly className: string = 'ToDouble';
}

export class ToDecfloat extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false };
  override get key(): string { return 'todecfloat'; }
  static readonly className: string = 'ToDecfloat';
}

export class TryToDecfloat extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false };
  override get key(): string { return 'trytodecfloat'; }
  static readonly className: string = 'TryToDecfloat';
}

export class ToFile extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'path': false, 'safe': false };
  override get key(): string { return 'tofile'; }
  static readonly className: string = 'ToFile';
}

export class CodePointsToBytes extends Func {
  override get key(): string { return 'codepointstobytes'; }
  static readonly className: string = 'CodePointsToBytes';
}

export class Columns extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'unpack': false };
  override get key(): string { return 'columns'; }
  static readonly className: string = 'Columns';
}

export class Convert extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'style': false, 'safe': false };
  override get key(): string { return 'convert'; }
  static readonly className: string = 'Convert';
}

export class ConvertToCharset extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'dest': true, 'source': false };
  override get key(): string { return 'converttocharset'; }
  static readonly className: string = 'ConvertToCharset';
}

export class ConvertTimezone extends Func {
  static readonly argTypes: Record<string, boolean> = { 'source_tz': false, 'target_tz': true, 'timestamp': true, 'options': false };
  override get key(): string { return 'converttimezone'; }
  static readonly className: string = 'ConvertTimezone';
}

export class CodePointsToString extends Func {
  override get key(): string { return 'codepointstostring'; }
  static readonly className: string = 'CodePointsToString';
}

export class GenerateSeries extends Func {
  static readonly argTypes: Record<string, boolean> = { 'start': true, 'end': true, 'step': false, 'is_end_exclusive': false };
  override get key(): string { return 'generateseries'; }
  static readonly className: string = 'GenerateSeries';
}

export class ExplodingGenerateSeries extends GenerateSeries {
  override get key(): string { return 'explodinggenerateseries'; }
  static readonly className: string = 'ExplodingGenerateSeries';
}

// Also extends: UDTF
export class Generator extends Func {
  static readonly argTypes: Record<string, boolean> = { 'rowcount': false, 'timelimit': false };
  override get key(): string { return 'generator'; }
  static readonly className: string = 'Generator';
}

export class ArrayAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'nulls_excluded': false };
  override get key(): string { return 'arrayagg'; }
  static readonly className: string = 'ArrayAgg';
}

export class ArrayUniqueAgg extends AggFunc {
  override get key(): string { return 'arrayuniqueagg'; }
  static readonly className: string = 'ArrayUniqueAgg';
}

export class AIAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  static readonly sqlNames: readonly string[] = ['AI_AGG'];
  override get key(): string { return 'aiagg'; }
  static readonly className: string = 'AIAgg';
}

export class AISummarizeAgg extends AggFunc {
  static readonly sqlNames: readonly string[] = ['AI_SUMMARIZE_AGG'];
  override get key(): string { return 'aisummarizeagg'; }
  static readonly className: string = 'AISummarizeAgg';
}

export class AIClassify extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'categories': true, 'config': false };
  static readonly sqlNames: readonly string[] = ['AI_CLASSIFY'];
  override get key(): string { return 'aiclassify'; }
  static readonly className: string = 'AIClassify';
}

export class ArrayAll extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'arrayall'; }
  static readonly className: string = 'ArrayAll';
}

export class ArrayAny extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'arrayany'; }
  static readonly className: string = 'ArrayAny';
}

export class ArrayAppend extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'null_propagation': false };
  override get key(): string { return 'arrayappend'; }
  static readonly className: string = 'ArrayAppend';
}

export class ArrayPrepend extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'null_propagation': false };
  override get key(): string { return 'arrayprepend'; }
  static readonly className: string = 'ArrayPrepend';
}

export class ArrayConcat extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'null_propagation': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['ARRAY_CONCAT', 'ARRAY_CAT'];
  override get key(): string { return 'arrayconcat'; }
  static readonly className: string = 'ArrayConcat';
}

export class ArrayConcatAgg extends AggFunc {
  override get key(): string { return 'arrayconcatagg'; }
  static readonly className: string = 'ArrayConcatAgg';
}

export class ArrayCompact extends Func {
  override get key(): string { return 'arraycompact'; }
  static readonly className: string = 'ArrayCompact';
}

export class ArrayInsert extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'position': true, 'expression': true, 'offset': false };
  override get key(): string { return 'arrayinsert'; }
  static readonly className: string = 'ArrayInsert';
}

export class ArrayRemoveAt extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'position': true };
  override get key(): string { return 'arrayremoveat'; }
  static readonly className: string = 'ArrayRemoveAt';
}

export class ArrayConstructCompact extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'arrayconstructcompact'; }
  static readonly className: string = 'ArrayConstructCompact';
}

// Also extends: Func
export class ArrayContains extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'ensure_variant': false };
  static readonly sqlNames: readonly string[] = ['ARRAY_CONTAINS', 'ARRAY_HAS'];
  override get key(): string { return 'arraycontains'; }
  static readonly className: string = 'ArrayContains';
  override get name(): string {
    const ctor = this.constructor as typeof ArrayContains;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class ArrayContainsAll extends Binary {
  static readonly sqlNames: readonly string[] = ['ARRAY_CONTAINS_ALL', 'ARRAY_HAS_ALL'];
  override get key(): string { return 'arraycontainsall'; }
  static readonly className: string = 'ArrayContainsAll';
  override get name(): string {
    const ctor = this.constructor as typeof ArrayContainsAll;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class ArrayFilter extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  static readonly sqlNames: readonly string[] = ['FILTER', 'ARRAY_FILTER'];
  override get key(): string { return 'arrayfilter'; }
  static readonly className: string = 'ArrayFilter';
}

export class ArrayFirst extends Func {
  override get key(): string { return 'arrayfirst'; }
  static readonly className: string = 'ArrayFirst';
}

export class ArrayLast extends Func {
  override get key(): string { return 'arraylast'; }
  static readonly className: string = 'ArrayLast';
}

export class ArrayReverse extends Func {
  override get key(): string { return 'arrayreverse'; }
  static readonly className: string = 'ArrayReverse';
}

export class ArraySlice extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'start': true, 'end': false, 'step': false };
  override get key(): string { return 'arrayslice'; }
  static readonly className: string = 'ArraySlice';
}

export class ArrayToString extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'null': false };
  static readonly sqlNames: readonly string[] = ['ARRAY_TO_STRING', 'ARRAY_JOIN'];
  override get key(): string { return 'arraytostring'; }
  static readonly className: string = 'ArrayToString';
}

export class ArrayIntersect extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['ARRAY_INTERSECT', 'ARRAY_INTERSECTION'];
  override get key(): string { return 'arrayintersect'; }
  static readonly className: string = 'ArrayIntersect';
}

export class StPoint extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'null': false };
  static readonly sqlNames: readonly string[] = ['ST_POINT', 'ST_MAKEPOINT'];
  override get key(): string { return 'stpoint'; }
  static readonly className: string = 'StPoint';
}

export class StDistance extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'use_spheroid': false };
  override get key(): string { return 'stdistance'; }
  static readonly className: string = 'StDistance';
}

export class String extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'zone': false };
  override get key(): string { return 'string'; }
  static readonly className: string = 'String';
}

export class StringToArray extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'null': false };
  static readonly sqlNames: readonly string[] = ['STRING_TO_ARRAY', 'SPLIT_BY_STRING', 'STRTOK_TO_ARRAY'];
  override get key(): string { return 'stringtoarray'; }
  static readonly className: string = 'StringToArray';
}

// Also extends: Func
export class ArrayOverlaps extends Binary {
  override get key(): string { return 'arrayoverlaps'; }
  static readonly className: string = 'ArrayOverlaps';
  override get name(): string {
    const ctor = this.constructor as typeof ArrayOverlaps;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class ArraySize extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  static readonly sqlNames: readonly string[] = ['ARRAY_SIZE', 'ARRAY_LENGTH'];
  override get key(): string { return 'arraysize'; }
  static readonly className: string = 'ArraySize';
}

export class ArraySort extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'arraysort'; }
  static readonly className: string = 'ArraySort';
}

export class ArraySum extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'arraysum'; }
  static readonly className: string = 'ArraySum';
}

export class ArrayUnionAgg extends AggFunc {
  override get key(): string { return 'arrayunionagg'; }
  static readonly className: string = 'ArrayUnionAgg';
}

export class ArraysZip extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'arrayszip'; }
  static readonly className: string = 'ArraysZip';
}

export class Avg extends AggFunc {
  override get key(): string { return 'avg'; }
  static readonly className: string = 'Avg';
}

export class AnyValue extends AggFunc {
  override get key(): string { return 'anyvalue'; }
  static readonly className: string = 'AnyValue';
}

export class Lag extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'offset': false, 'default': false };
  override get key(): string { return 'lag'; }
  static readonly className: string = 'Lag';
}

export class Lead extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'offset': false, 'default': false };
  override get key(): string { return 'lead'; }
  static readonly className: string = 'Lead';
}

export class First extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'first'; }
  static readonly className: string = 'First';
}

export class Last extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'last'; }
  static readonly className: string = 'Last';
}

export class FirstValue extends AggFunc {
  override get key(): string { return 'firstvalue'; }
  static readonly className: string = 'FirstValue';
}

export class LastValue extends AggFunc {
  override get key(): string { return 'lastvalue'; }
  static readonly className: string = 'LastValue';
}

export class NthValue extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'offset': true, 'from_first': false };
  override get key(): string { return 'nthvalue'; }
  static readonly className: string = 'NthValue';
}

export class ObjectAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'objectagg'; }
  static readonly className: string = 'ObjectAgg';
}

export class Case extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'ifs': true, 'default': false };
  override get key(): string { return 'case'; }
  static readonly className: string = 'Case';
}

export class Cast extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'to': true, 'format': false, 'safe': false, 'action': false, 'default': false };
  override get key(): string { return 'cast'; }
  static readonly className: string = 'Cast';
}

export class TryCast extends Cast {
  static readonly argTypes: Record<string, boolean> = { 'requires_string': false };
  override get key(): string { return 'trycast'; }
  static readonly className: string = 'TryCast';
}

export class JSONCast extends Cast {
  override get key(): string { return 'jsoncast'; }
  static readonly className: string = 'JSONCast';
}

export class JustifyDays extends Func {
  override get key(): string { return 'justifydays'; }
  static readonly className: string = 'JustifyDays';
}

export class JustifyHours extends Func {
  override get key(): string { return 'justifyhours'; }
  static readonly className: string = 'JustifyHours';
}

export class JustifyInterval extends Func {
  override get key(): string { return 'justifyinterval'; }
  static readonly className: string = 'JustifyInterval';
}

export class Try extends Func {
  override get key(): string { return 'try'; }
  static readonly className: string = 'Try';
}

export class CastToStrType extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'to': true };
  override get key(): string { return 'casttostrtype'; }
  static readonly className: string = 'CastToStrType';
}

export class CheckJson extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'checkjson'; }
  static readonly className: string = 'CheckJson';
}

export class CheckXml extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'disable_auto_convert': false };
  override get key(): string { return 'checkxml'; }
  static readonly className: string = 'CheckXml';
}

export class TranslateCharacters extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'with_error': false };
  get key(): string { return 'translatecharacters'; }
  static readonly className: string = 'TranslateCharacters';
}

// Also extends: Func
export class Collate extends Binary {
  override get key(): string { return 'collate'; }
  static readonly className: string = 'Collate';
  override get name(): string {
    const ctor = this.constructor as typeof Collate;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class Collation extends Func {
  override get key(): string { return 'collation'; }
  static readonly className: string = 'Collation';
}

export class Ceil extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'decimals': false, 'to': false };
  static readonly sqlNames: readonly string[] = ['CEIL', 'CEILING'];
  override get key(): string { return 'ceil'; }
  static readonly className: string = 'Ceil';
}

export class Coalesce extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'is_nvl': false, 'is_null': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['COALESCE', 'IFNULL', 'NVL'];
  override get key(): string { return 'coalesce'; }
  static readonly className: string = 'Coalesce';
}

export class Chr extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'charset': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['CHR', 'CHAR'];
  override get key(): string { return 'chr'; }
  static readonly className: string = 'Chr';
}

export class Concat extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'safe': false, 'coalesce': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'concat'; }
  static readonly className: string = 'Concat';
}

export class ConcatWs extends Concat {
  static readonly sqlNames: readonly string[] = ['CONCAT_WS'];
  override get key(): string { return 'concatws'; }
  static readonly className: string = 'ConcatWs';
}

export class Contains extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'json_scope': false };
  override get key(): string { return 'contains'; }
  static readonly className: string = 'Contains';
}

export class ConnectByRoot extends Func {
  override get key(): string { return 'connectbyroot'; }
  static readonly className: string = 'ConnectByRoot';
}

export class Count extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': false, 'big_int': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'count'; }
  static readonly className: string = 'Count';
}

export class CountIf extends AggFunc {
  static readonly sqlNames: readonly string[] = ['COUNT_IF', 'COUNTIF'];
  override get key(): string { return 'countif'; }
  static readonly className: string = 'CountIf';
}

export class Cbrt extends Func {
  override get key(): string { return 'cbrt'; }
  static readonly className: string = 'Cbrt';
}

export class CurrentAccount extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentaccount'; }
  static readonly className: string = 'CurrentAccount';
}

export class CurrentAccountName extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentaccountname'; }
  static readonly className: string = 'CurrentAccountName';
}

export class CurrentAvailableRoles extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentavailableroles'; }
  static readonly className: string = 'CurrentAvailableRoles';
}

export class CurrentClient extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentclient'; }
  static readonly className: string = 'CurrentClient';
}

export class CurrentIpAddress extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentipaddress'; }
  static readonly className: string = 'CurrentIpAddress';
}

export class CurrentDatabase extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentdatabase'; }
  static readonly className: string = 'CurrentDatabase';
}

export class CurrentSchemas extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'currentschemas'; }
  static readonly className: string = 'CurrentSchemas';
}

export class CurrentSecondaryRoles extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentsecondaryroles'; }
  static readonly className: string = 'CurrentSecondaryRoles';
}

export class CurrentSession extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentsession'; }
  static readonly className: string = 'CurrentSession';
}

export class CurrentStatement extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentstatement'; }
  static readonly className: string = 'CurrentStatement';
}

export class CurrentVersion extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentversion'; }
  static readonly className: string = 'CurrentVersion';
}

export class CurrentTransaction extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currenttransaction'; }
  static readonly className: string = 'CurrentTransaction';
}

export class CurrentWarehouse extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentwarehouse'; }
  static readonly className: string = 'CurrentWarehouse';
}

export class CurrentDate extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'currentdate'; }
  static readonly className: string = 'CurrentDate';
}

export class CurrentDatetime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'currentdatetime'; }
  static readonly className: string = 'CurrentDatetime';
}

export class CurrentTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'currenttime'; }
  static readonly className: string = 'CurrentTime';
}

export class Localtime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'localtime'; }
  static readonly className: string = 'Localtime';
}

export class Localtimestamp extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'localtimestamp'; }
  static readonly className: string = 'Localtimestamp';
}

export class Systimestamp extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'systimestamp'; }
  static readonly className: string = 'Systimestamp';
}

export class CurrentTimestamp extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'sysdate': false };
  override get key(): string { return 'currenttimestamp'; }
  static readonly className: string = 'CurrentTimestamp';
}

export class CurrentTimestampLTZ extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currenttimestampltz'; }
  static readonly className: string = 'CurrentTimestampLTZ';
}

export class CurrentTimezone extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currenttimezone'; }
  static readonly className: string = 'CurrentTimezone';
}

export class CurrentOrganizationName extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentorganizationname'; }
  static readonly className: string = 'CurrentOrganizationName';
}

export class CurrentSchema extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'currentschema'; }
  static readonly className: string = 'CurrentSchema';
}

export class CurrentUser extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'currentuser'; }
  static readonly className: string = 'CurrentUser';
}

export class CurrentCatalog extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentcatalog'; }
  static readonly className: string = 'CurrentCatalog';
}

export class CurrentRegion extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentregion'; }
  static readonly className: string = 'CurrentRegion';
}

export class CurrentRole extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentrole'; }
  static readonly className: string = 'CurrentRole';
}

export class CurrentRoleType extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentroletype'; }
  static readonly className: string = 'CurrentRoleType';
}

export class CurrentOrganizationUser extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'currentorganizationuser'; }
  static readonly className: string = 'CurrentOrganizationUser';
}

export class SessionUser extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'sessionuser'; }
  static readonly className: string = 'SessionUser';
}

export class UtcDate extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'utcdate'; }
  static readonly className: string = 'UtcDate';
}

export class UtcTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'utctime'; }
  static readonly className: string = 'UtcTime';
}

export class UtcTimestamp extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'utctimestamp'; }
  static readonly className: string = 'UtcTimestamp';
}

// Also extends: IntervalOp
export class DateAdd extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'dateadd'; }
  static readonly className: string = 'DateAdd';
}

// Also extends: IntervalOp
export class DateBin extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false, 'zone': false, 'origin': false };
  override get key(): string { return 'datebin'; }
  static readonly className: string = 'DateBin';
}

// Also extends: IntervalOp
export class DateSub extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'datesub'; }
  static readonly className: string = 'DateSub';
}

// Also extends: TimeUnit
export class DateDiff extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false, 'zone': false, 'big_int': false, 'date_part_boundary': false };
  static readonly sqlNames: readonly string[] = ['DATEDIFF', 'DATE_DIFF'];
  override get key(): string { return 'datediff'; }
  static readonly className: string = 'DateDiff';
}

export class DateTrunc extends Func {
  static readonly argTypes: Record<string, boolean> = { 'unit': true, 'this': true, 'zone': false, 'input_type_preserved': false };
  override get key(): string { return 'datetrunc'; }
  static readonly className: string = 'DateTrunc';
}

export class Datetime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'datetime'; }
  static readonly className: string = 'Datetime';
}

// Also extends: IntervalOp
export class DatetimeAdd extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'datetimeadd'; }
  static readonly className: string = 'DatetimeAdd';
}

// Also extends: IntervalOp
export class DatetimeSub extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'datetimesub'; }
  static readonly className: string = 'DatetimeSub';
}

// Also extends: TimeUnit
export class DatetimeDiff extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'datetimediff'; }
  static readonly className: string = 'DatetimeDiff';
}

// Also extends: TimeUnit
export class DatetimeTrunc extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'unit': true, 'zone': false };
  override get key(): string { return 'datetimetrunc'; }
  static readonly className: string = 'DatetimeTrunc';
}

export class DateFromUnixDate extends Func {
  override get key(): string { return 'datefromunixdate'; }
  static readonly className: string = 'DateFromUnixDate';
}

export class DayOfWeek extends Func {
  static readonly sqlNames: readonly string[] = ['DAY_OF_WEEK', 'DAYOFWEEK'];
  override get key(): string { return 'dayofweek'; }
  static readonly className: string = 'DayOfWeek';
}

export class DayOfWeekIso extends Func {
  static readonly sqlNames: readonly string[] = ['DAYOFWEEK_ISO', 'ISODOW'];
  override get key(): string { return 'dayofweekiso'; }
  static readonly className: string = 'DayOfWeekIso';
}

export class DayOfMonth extends Func {
  static readonly sqlNames: readonly string[] = ['DAY_OF_MONTH', 'DAYOFMONTH'];
  override get key(): string { return 'dayofmonth'; }
  static readonly className: string = 'DayOfMonth';
}

export class DayOfYear extends Func {
  static readonly sqlNames: readonly string[] = ['DAY_OF_YEAR', 'DAYOFYEAR'];
  override get key(): string { return 'dayofyear'; }
  static readonly className: string = 'DayOfYear';
}

export class Dayname extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'abbreviated': false };
  override get key(): string { return 'dayname'; }
  static readonly className: string = 'Dayname';
}

export class ToDays extends Func {
  override get key(): string { return 'todays'; }
  static readonly className: string = 'ToDays';
}

export class WeekOfYear extends Func {
  static readonly sqlNames: readonly string[] = ['WEEK_OF_YEAR', 'WEEKOFYEAR'];
  override get key(): string { return 'weekofyear'; }
  static readonly className: string = 'WeekOfYear';
}

export class YearOfWeek extends Func {
  static readonly sqlNames: readonly string[] = ['YEAR_OF_WEEK', 'YEAROFWEEK'];
  override get key(): string { return 'yearofweek'; }
  static readonly className: string = 'YearOfWeek';
}

export class YearOfWeekIso extends Func {
  static readonly sqlNames: readonly string[] = ['YEAR_OF_WEEK_ISO', 'YEAROFWEEKISO'];
  override get key(): string { return 'yearofweekiso'; }
  static readonly className: string = 'YearOfWeekIso';
}

export class MonthsBetween extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'roundoff': false };
  override get key(): string { return 'monthsbetween'; }
  static readonly className: string = 'MonthsBetween';
}

export class MakeInterval extends Func {
  static readonly argTypes: Record<string, boolean> = { 'year': false, 'month': false, 'week': false, 'day': false, 'hour': false, 'minute': false, 'second': false };
  override get key(): string { return 'makeinterval'; }
  static readonly className: string = 'MakeInterval';
}

// Also extends: TimeUnit
export class LastDay extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'unit': false };
  static readonly sqlNames: readonly string[] = ['LAST_DAY', 'LAST_DAY_OF_MONTH'];
  override get key(): string { return 'lastday'; }
  static readonly className: string = 'LastDay';
}

export class PreviousDay extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'previousday'; }
  static readonly className: string = 'PreviousDay';
}

export class LaxBool extends Func {
  override get key(): string { return 'laxbool'; }
  static readonly className: string = 'LaxBool';
}

export class LaxFloat64 extends Func {
  override get key(): string { return 'laxfloat64'; }
  static readonly className: string = 'LaxFloat64';
}

export class LaxInt64 extends Func {
  override get key(): string { return 'laxint64'; }
  static readonly className: string = 'LaxInt64';
}

export class LaxString extends Func {
  override get key(): string { return 'laxstring'; }
  static readonly className: string = 'LaxString';
}

export class Extract extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'extract'; }
  static readonly className: string = 'Extract';
}

// Also extends: SubqueryPredicate
export class Exists extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'exists'; }
  static readonly className: string = 'Exists';
}

export class Elt extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'elt'; }
  static readonly className: string = 'Elt';
}

export class Timestamp extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'zone': false, 'with_tz': false };
  override get key(): string { return 'timestamp'; }
  static readonly className: string = 'Timestamp';
}

// Also extends: TimeUnit
export class TimestampAdd extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'timestampadd'; }
  static readonly className: string = 'TimestampAdd';
}

// Also extends: TimeUnit
export class TimestampSub extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'timestampsub'; }
  static readonly className: string = 'TimestampSub';
}

// Also extends: TimeUnit
export class TimestampDiff extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  static readonly sqlNames: readonly string[] = ['TIMESTAMPDIFF', 'TIMESTAMP_DIFF'];
  override get key(): string { return 'timestampdiff'; }
  static readonly className: string = 'TimestampDiff';
}

// Also extends: TimeUnit
export class TimestampTrunc extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'unit': true, 'zone': false, 'input_type_preserved': false };
  override get key(): string { return 'timestamptrunc'; }
  static readonly className: string = 'TimestampTrunc';
}

// Also extends: TimeUnit
export class TimeSlice extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': true, 'kind': false };
  override get key(): string { return 'timeslice'; }
  static readonly className: string = 'TimeSlice';
}

// Also extends: TimeUnit
export class TimeAdd extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'timeadd'; }
  static readonly className: string = 'TimeAdd';
}

// Also extends: TimeUnit
export class TimeSub extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'timesub'; }
  static readonly className: string = 'TimeSub';
}

// Also extends: TimeUnit
export class TimeDiff extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'timediff'; }
  static readonly className: string = 'TimeDiff';
}

// Also extends: TimeUnit
export class TimeTrunc extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'unit': true, 'zone': false };
  override get key(): string { return 'timetrunc'; }
  static readonly className: string = 'TimeTrunc';
}

export class DateFromParts extends Func {
  static readonly argTypes: Record<string, boolean> = { 'year': true, 'month': false, 'day': false, 'allow_overflow': false };
  static readonly sqlNames: readonly string[] = ['DATE_FROM_PARTS', 'DATEFROMPARTS'];
  override get key(): string { return 'datefromparts'; }
  static readonly className: string = 'DateFromParts';
}

export class TimeFromParts extends Func {
  static readonly argTypes: Record<string, boolean> = { 'hour': true, 'min': true, 'sec': true, 'nano': false, 'fractions': false, 'precision': false, 'overflow': false };
  static readonly sqlNames: readonly string[] = ['TIME_FROM_PARTS', 'TIMEFROMPARTS'];
  override get key(): string { return 'timefromparts'; }
  static readonly className: string = 'TimeFromParts';
}

export class DateStrToDate extends Func {
  override get key(): string { return 'datestrtodate'; }
  static readonly className: string = 'DateStrToDate';
}

export class DateToDateStr extends Func {
  override get key(): string { return 'datetodatestr'; }
  static readonly className: string = 'DateToDateStr';
}

export class DateToDi extends Func {
  override get key(): string { return 'datetodi'; }
  static readonly className: string = 'DateToDi';
}

export class Date extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'zone': false, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'date'; }
  static readonly className: string = 'Date';
}

export class Day extends Func {
  override get key(): string { return 'day'; }
  static readonly className: string = 'Day';
}

export class Decode extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'charset': true, 'replace': false };
  override get key(): string { return 'decode'; }
  static readonly className: string = 'Decode';
}

export class DecodeCase extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'decodecase'; }
  static readonly className: string = 'DecodeCase';
}

export class Decrypt extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'passphrase': true, 'aad': false, 'encryption_method': false, 'safe': false };
  override get key(): string { return 'decrypt'; }
  static readonly className: string = 'Decrypt';
}

export class DecryptRaw extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'key': true, 'iv': true, 'aad': false, 'encryption_method': false, 'aead': false, 'safe': false };
  override get key(): string { return 'decryptraw'; }
  static readonly className: string = 'DecryptRaw';
}

export class DenseRank extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'denserank'; }
  static readonly className: string = 'DenseRank';
}

export class DiToDate extends Func {
  override get key(): string { return 'ditodate'; }
  static readonly className: string = 'DiToDate';
}

export class Encode extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'charset': true };
  override get key(): string { return 'encode'; }
  static readonly className: string = 'Encode';
}

export class Encrypt extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'passphrase': true, 'aad': false, 'encryption_method': false };
  override get key(): string { return 'encrypt'; }
  static readonly className: string = 'Encrypt';
}

export class EncryptRaw extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'key': true, 'iv': true, 'aad': false, 'encryption_method': false };
  override get key(): string { return 'encryptraw'; }
  static readonly className: string = 'EncryptRaw';
}

export class EqualNull extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'equalnull'; }
  static readonly className: string = 'EqualNull';
}

export class Exp extends Func {
  override get key(): string { return 'exp'; }
  static readonly className: string = 'Exp';
}

export class Factorial extends Func {
  override get key(): string { return 'factorial'; }
  static readonly className: string = 'Factorial';
}

// Also extends: UDTF
export class Explode extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'explode'; }
  static readonly className: string = 'Explode';
}

export class Inline extends Func {
  override get key(): string { return 'inline'; }
  static readonly className: string = 'Inline';
}

export class ExplodeOuter extends Explode {
  override get key(): string { return 'explodeouter'; }
  static readonly className: string = 'ExplodeOuter';
}

export class Posexplode extends Explode {
  override get key(): string { return 'posexplode'; }
  static readonly className: string = 'Posexplode';
}

// Also extends: ExplodeOuter
export class PosexplodeOuter extends Posexplode {
  override get key(): string { return 'posexplodeouter'; }
  static readonly className: string = 'PosexplodeOuter';
}

export class PositionalColumn extends Expression {
  get key(): string { return 'positionalcolumn'; }
  static readonly className: string = 'PositionalColumn';
}

// Also extends: UDTF
export class Unnest extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'alias': false, 'offset': false, 'explode_array': false };
  override get key(): string { return 'unnest'; }
  static readonly className: string = 'Unnest';
}

export class Floor extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'decimals': false, 'to': false };
  override get key(): string { return 'floor'; }
  static readonly className: string = 'Floor';
}

export class FromBase32 extends Func {
  override get key(): string { return 'frombase32'; }
  static readonly className: string = 'FromBase32';
}

export class FromBase64 extends Func {
  override get key(): string { return 'frombase64'; }
  static readonly className: string = 'FromBase64';
}

export class ToBase32 extends Func {
  override get key(): string { return 'tobase32'; }
  static readonly className: string = 'ToBase32';
}

export class ToBase64 extends Func {
  override get key(): string { return 'tobase64'; }
  static readonly className: string = 'ToBase64';
}

export class ToBinary extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'safe': false };
  override get key(): string { return 'tobinary'; }
  static readonly className: string = 'ToBinary';
}

export class Base64DecodeBinary extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alphabet': false };
  override get key(): string { return 'base64decodebinary'; }
  static readonly className: string = 'Base64DecodeBinary';
}

export class Base64DecodeString extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alphabet': false };
  override get key(): string { return 'base64decodestring'; }
  static readonly className: string = 'Base64DecodeString';
}

export class Base64Encode extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'max_line_length': false, 'alphabet': false };
  override get key(): string { return 'base64encode'; }
  static readonly className: string = 'Base64Encode';
}

export class TryBase64DecodeBinary extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alphabet': false };
  override get key(): string { return 'trybase64decodebinary'; }
  static readonly className: string = 'TryBase64DecodeBinary';
}

export class TryBase64DecodeString extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'alphabet': false };
  override get key(): string { return 'trybase64decodestring'; }
  static readonly className: string = 'TryBase64DecodeString';
}

export class TryHexDecodeBinary extends Func {
  override get key(): string { return 'tryhexdecodebinary'; }
  static readonly className: string = 'TryHexDecodeBinary';
}

export class TryHexDecodeString extends Func {
  override get key(): string { return 'tryhexdecodestring'; }
  static readonly className: string = 'TryHexDecodeString';
}

export class FromISO8601Timestamp extends Func {
  static readonly sqlNames: readonly string[] = ['FROM_ISO8601_TIMESTAMP'];
  override get key(): string { return 'fromiso8601timestamp'; }
  static readonly className: string = 'FromISO8601Timestamp';
}

export class GapFill extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'ts_column': true, 'bucket_width': true, 'partitioning_columns': false, 'value_columns': false, 'origin': false, 'ignore_nulls': false };
  override get key(): string { return 'gapfill'; }
  static readonly className: string = 'GapFill';
}

export class GenerateDateArray extends Func {
  static readonly argTypes: Record<string, boolean> = { 'start': true, 'end': true, 'step': false };
  override get key(): string { return 'generatedatearray'; }
  static readonly className: string = 'GenerateDateArray';
}

export class GenerateTimestampArray extends Func {
  static readonly argTypes: Record<string, boolean> = { 'start': true, 'end': true, 'step': true };
  override get key(): string { return 'generatetimestamparray'; }
  static readonly className: string = 'GenerateTimestampArray';
}

export class GetExtract extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'getextract'; }
  static readonly className: string = 'GetExtract';
}

export class Getbit extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'zero_is_msb': false };
  static readonly sqlNames: readonly string[] = ['GETBIT', 'GET_BIT'];
  override get key(): string { return 'getbit'; }
  static readonly className: string = 'Getbit';
}

export class Greatest extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'ignore_nulls': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'greatest'; }
  static readonly className: string = 'Greatest';
}

export class OverflowTruncateBehavior extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'with_count': true };
  get key(): string { return 'overflowtruncatebehavior'; }
  static readonly className: string = 'OverflowTruncateBehavior';
}

export class GroupConcat extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'separator': false, 'on_overflow': false };
  override get key(): string { return 'groupconcat'; }
  static readonly className: string = 'GroupConcat';
}

export class Hex extends Func {
  override get key(): string { return 'hex'; }
  static readonly className: string = 'Hex';
}

export class HexDecodeString extends Func {
  override get key(): string { return 'hexdecodestring'; }
  static readonly className: string = 'HexDecodeString';
}

export class HexEncode extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'case': false };
  override get key(): string { return 'hexencode'; }
  static readonly className: string = 'HexEncode';
}

export class Hour extends Func {
  override get key(): string { return 'hour'; }
  static readonly className: string = 'Hour';
}

export class Minute extends Func {
  override get key(): string { return 'minute'; }
  static readonly className: string = 'Minute';
}

export class Second extends Func {
  override get key(): string { return 'second'; }
  static readonly className: string = 'Second';
}

export class Compress extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'method': false };
  override get key(): string { return 'compress'; }
  static readonly className: string = 'Compress';
}

export class DecompressBinary extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'method': true };
  override get key(): string { return 'decompressbinary'; }
  static readonly className: string = 'DecompressBinary';
}

export class DecompressString extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'method': true };
  override get key(): string { return 'decompressstring'; }
  static readonly className: string = 'DecompressString';
}

export class LowerHex extends Hex {
  override get key(): string { return 'lowerhex'; }
  static readonly className: string = 'LowerHex';
}

// Also extends: Func
export class And extends Connector {
  override get key(): string { return 'and'; }
  static readonly className: string = 'And';
  override get name(): string {
    const ctor = this.constructor as typeof And;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class Or extends Connector {
  override get key(): string { return 'or'; }
  static readonly className: string = 'Or';
  override get name(): string {
    const ctor = this.constructor as typeof Or;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class Xor extends Connector {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expression': false, 'expressions': false, 'round_input': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'xor'; }
  static readonly className: string = 'Xor';
  override get name(): string {
    const ctor = this.constructor as typeof Xor;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class If extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'true': true, 'false': false };
  static readonly sqlNames: readonly string[] = ['IF', 'IIF'];
  override get key(): string { return 'if'; }
  static readonly className: string = 'If';
}

export class Nullif extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'nullif'; }
  static readonly className: string = 'Nullif';
}

export class Initcap extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'initcap'; }
  static readonly className: string = 'Initcap';
}

export class IsAscii extends Func {
  override get key(): string { return 'isascii'; }
  static readonly className: string = 'IsAscii';
}

export class IsNan extends Func {
  static readonly sqlNames: readonly string[] = ['IS_NAN', 'ISNAN'];
  override get key(): string { return 'isnan'; }
  static readonly className: string = 'IsNan';
}

export class Int64 extends Func {
  override get key(): string { return 'int64'; }
  static readonly className: string = 'Int64';
}

export class IsInf extends Func {
  static readonly sqlNames: readonly string[] = ['IS_INF', 'ISINF'];
  override get key(): string { return 'isinf'; }
  static readonly className: string = 'IsInf';
}

export class IsNullValue extends Func {
  override get key(): string { return 'isnullvalue'; }
  static readonly className: string = 'IsNullValue';
}

export class IsArray extends Func {
  override get key(): string { return 'isarray'; }
  static readonly className: string = 'IsArray';
}

export class JSON extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'with_': false, 'unique': false };
  get key(): string { return 'json'; }
  static readonly className: string = 'JSON';
}

export class JSONPath extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true, 'escape': false };
  get key(): string { return 'jsonpath'; }
  static readonly className: string = 'JSONPath';
}

export class JSONPathPart extends Expression {
  static readonly argTypes: Record<string, boolean> = {};
  get key(): string { return 'jsonpathpart'; }
  static readonly className: string = 'JSONPathPart';
}

export class JSONPathFilter extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'jsonpathfilter'; }
  static readonly className: string = 'JSONPathFilter';
}

export class JSONPathKey extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'jsonpathkey'; }
  static readonly className: string = 'JSONPathKey';
}

export class JSONPathRecursive extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'jsonpathrecursive'; }
  static readonly className: string = 'JSONPathRecursive';
}

export class JSONPathRoot extends JSONPathPart {
  override get key(): string { return 'jsonpathroot'; }
  static readonly className: string = 'JSONPathRoot';
}

export class JSONPathScript extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'jsonpathscript'; }
  static readonly className: string = 'JSONPathScript';
}

export class JSONPathSlice extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'start': false, 'end': false, 'step': false };
  override get key(): string { return 'jsonpathslice'; }
  static readonly className: string = 'JSONPathSlice';
}

export class JSONPathSelector extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'jsonpathselector'; }
  static readonly className: string = 'JSONPathSelector';
}

export class JSONPathSubscript extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'this': true };
  override get key(): string { return 'jsonpathsubscript'; }
  static readonly className: string = 'JSONPathSubscript';
}

export class JSONPathUnion extends JSONPathPart {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  override get key(): string { return 'jsonpathunion'; }
  static readonly className: string = 'JSONPathUnion';
}

export class JSONPathWildcard extends JSONPathPart {
  override get key(): string { return 'jsonpathwildcard'; }
  static readonly className: string = 'JSONPathWildcard';
}

export class FormatJson extends Expression {
  get key(): string { return 'formatjson'; }
  static readonly className: string = 'FormatJson';
}

export class Format extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'format'; }
  static readonly className: string = 'Format';
}

export class JSONKeys extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'expressions': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['JSON_KEYS'];
  override get key(): string { return 'jsonkeys'; }
  static readonly className: string = 'JSONKeys';
}

export class JSONKeyValue extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'jsonkeyvalue'; }
  static readonly className: string = 'JSONKeyValue';
}

export class JSONKeysAtDepth extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'mode': false };
  override get key(): string { return 'jsonkeysatdepth'; }
  static readonly className: string = 'JSONKeysAtDepth';
}

export class JSONObject extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'null_handling': false, 'unique_keys': false, 'return_type': false, 'encoding': false };
  override get key(): string { return 'jsonobject'; }
  static readonly className: string = 'JSONObject';
}

export class JSONObjectAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'null_handling': false, 'unique_keys': false, 'return_type': false, 'encoding': false };
  override get key(): string { return 'jsonobjectagg'; }
  static readonly className: string = 'JSONObjectAgg';
}

export class JSONBObjectAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'jsonbobjectagg'; }
  static readonly className: string = 'JSONBObjectAgg';
}

export class JSONArray extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false, 'null_handling': false, 'return_type': false, 'strict': false };
  override get key(): string { return 'jsonarray'; }
  static readonly className: string = 'JSONArray';
}

export class JSONArrayAgg extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'order': false, 'null_handling': false, 'return_type': false, 'strict': false };
  override get key(): string { return 'jsonarrayagg'; }
  static readonly className: string = 'JSONArrayAgg';
}

export class JSONExists extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'path': true, 'passing': false, 'on_condition': false, 'from_dcolonqmark': false };
  override get key(): string { return 'jsonexists'; }
  static readonly className: string = 'JSONExists';
}

export class JSONColumnDef extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'kind': false, 'path': false, 'nested_schema': false, 'ordinality': false };
  get key(): string { return 'jsoncolumndef'; }
  static readonly className: string = 'JSONColumnDef';
}

export class JSONSchema extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'jsonschema'; }
  static readonly className: string = 'JSONSchema';
}

export class JSONSet extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['JSON_SET'];
  override get key(): string { return 'jsonset'; }
  static readonly className: string = 'JSONSet';
}

export class JSONStripNulls extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'include_arrays': false, 'remove_empty': false };
  static readonly sqlNames: readonly string[] = ['JSON_STRIP_NULLS'];
  override get key(): string { return 'jsonstripnulls'; }
  static readonly className: string = 'JSONStripNulls';
}

export class JSONValue extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'path': true, 'returning': false, 'on_condition': false };
  get key(): string { return 'jsonvalue'; }
  static readonly className: string = 'JSONValue';
}

export class JSONValueArray extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'jsonvaluearray'; }
  static readonly className: string = 'JSONValueArray';
}

export class JSONRemove extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['JSON_REMOVE'];
  override get key(): string { return 'jsonremove'; }
  static readonly className: string = 'JSONRemove';
}

export class JSONTable extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'schema': true, 'path': false, 'error_handling': false, 'empty_handling': false };
  override get key(): string { return 'jsontable'; }
  static readonly className: string = 'JSONTable';
}

export class JSONType extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  static readonly sqlNames: readonly string[] = ['JSON_TYPE'];
  override get key(): string { return 'jsontype'; }
  static readonly className: string = 'JSONType';
}

export class ObjectInsert extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'key': true, 'value': true, 'update_flag': false };
  override get key(): string { return 'objectinsert'; }
  static readonly className: string = 'ObjectInsert';
}

export class OpenJSONColumnDef extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'kind': true, 'path': false, 'as_json': false };
  get key(): string { return 'openjsoncolumndef'; }
  static readonly className: string = 'OpenJSONColumnDef';
}

export class OpenJSON extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'path': false, 'expressions': false };
  override get key(): string { return 'openjson'; }
  static readonly className: string = 'OpenJSON';
}

// Also extends: Func
export class JSONBContains extends Binary {
  static readonly sqlNames: readonly string[] = ['JSONB_CONTAINS'];
  override get key(): string { return 'jsonbcontains'; }
  static readonly className: string = 'JSONBContains';
  override get name(): string {
    const ctor = this.constructor as typeof JSONBContains;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class JSONBContainsAnyTopKeys extends Binary {
  override get key(): string { return 'jsonbcontainsanytopkeys'; }
  static readonly className: string = 'JSONBContainsAnyTopKeys';
  override get name(): string {
    const ctor = this.constructor as typeof JSONBContainsAnyTopKeys;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class JSONBContainsAllTopKeys extends Binary {
  override get key(): string { return 'jsonbcontainsalltopkeys'; }
  static readonly className: string = 'JSONBContainsAllTopKeys';
  override get name(): string {
    const ctor = this.constructor as typeof JSONBContainsAllTopKeys;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class JSONBExists extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'path': true };
  static readonly sqlNames: readonly string[] = ['JSONB_EXISTS'];
  override get key(): string { return 'jsonbexists'; }
  static readonly className: string = 'JSONBExists';
}

// Also extends: Func
export class JSONBDeleteAtPath extends Binary {
  override get key(): string { return 'jsonbdeleteatpath'; }
  static readonly className: string = 'JSONBDeleteAtPath';
  override get name(): string {
    const ctor = this.constructor as typeof JSONBDeleteAtPath;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class JSONExtract extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'only_json_types': false, 'expressions': false, 'variant_extract': false, 'json_query': false, 'option': false, 'quote': false, 'on_condition': false, 'requires_json': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['JSON_EXTRACT'];
  override get key(): string { return 'jsonextract'; }
  static readonly className: string = 'JSONExtract';
  override get name(): string {
    const ctor = this.constructor as typeof JSONExtract;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class JSONExtractQuote extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'option': true, 'scalar': false };
  get key(): string { return 'jsonextractquote'; }
  static readonly className: string = 'JSONExtractQuote';
}

export class JSONExtractArray extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  static readonly sqlNames: readonly string[] = ['JSON_EXTRACT_ARRAY'];
  override get key(): string { return 'jsonextractarray'; }
  static readonly className: string = 'JSONExtractArray';
}

// Also extends: Func
export class JSONExtractScalar extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'only_json_types': false, 'expressions': false, 'json_type': false, 'scalar_only': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['JSON_EXTRACT_SCALAR'];
  override get key(): string { return 'jsonextractscalar'; }
  static readonly className: string = 'JSONExtractScalar';
  override get name(): string {
    const ctor = this.constructor as typeof JSONExtractScalar;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class JSONBExtract extends Binary {
  static readonly sqlNames: readonly string[] = ['JSONB_EXTRACT'];
  override get key(): string { return 'jsonbextract'; }
  static readonly className: string = 'JSONBExtract';
  override get name(): string {
    const ctor = this.constructor as typeof JSONBExtract;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class JSONBExtractScalar extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'json_type': false };
  static readonly sqlNames: readonly string[] = ['JSONB_EXTRACT_SCALAR'];
  override get key(): string { return 'jsonbextractscalar'; }
  static readonly className: string = 'JSONBExtractScalar';
  override get name(): string {
    const ctor = this.constructor as typeof JSONBExtractScalar;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class JSONFormat extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'options': false, 'is_json': false, 'to_json': false };
  static readonly sqlNames: readonly string[] = ['JSON_FORMAT'];
  override get key(): string { return 'jsonformat'; }
  static readonly className: string = 'JSONFormat';
}

export class JSONArrayAppend extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['JSON_ARRAY_APPEND'];
  override get key(): string { return 'jsonarrayappend'; }
  static readonly className: string = 'JSONArrayAppend';
}

// Also extends: Predicate, Func
export class JSONArrayContains extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'json_type': false };
  static readonly sqlNames: readonly string[] = ['JSON_ARRAY_CONTAINS'];
  override get key(): string { return 'jsonarraycontains'; }
  static readonly className: string = 'JSONArrayContains';
  override get name(): string {
    const ctor = this.constructor as typeof JSONArrayContains;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class JSONArrayInsert extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['JSON_ARRAY_INSERT'];
  override get key(): string { return 'jsonarrayinsert'; }
  static readonly className: string = 'JSONArrayInsert';
}

export class ParseBignumeric extends Func {
  override get key(): string { return 'parsebignumeric'; }
  static readonly className: string = 'ParseBignumeric';
}

export class ParseNumeric extends Func {
  override get key(): string { return 'parsenumeric'; }
  static readonly className: string = 'ParseNumeric';
}

export class ParseJSON extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'safe': false };
  static readonly sqlNames: readonly string[] = ['PARSE_JSON', 'JSON_PARSE'];
  override get key(): string { return 'parsejson'; }
  static readonly className: string = 'ParseJSON';
}

export class ParseUrl extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'part_to_extract': false, 'key': false, 'permissive': false };
  override get key(): string { return 'parseurl'; }
  static readonly className: string = 'ParseUrl';
}

export class ParseIp extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'type': true, 'permissive': false };
  override get key(): string { return 'parseip'; }
  static readonly className: string = 'ParseIp';
}

export class ParseTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': true };
  override get key(): string { return 'parsetime'; }
  static readonly className: string = 'ParseTime';
}

export class ParseDatetime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'zone': false };
  override get key(): string { return 'parsedatetime'; }
  static readonly className: string = 'ParseDatetime';
}

export class Least extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'ignore_nulls': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'least'; }
  static readonly className: string = 'Least';
}

export class Left extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'left'; }
  static readonly className: string = 'Left';
}

export class Right extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'right'; }
  static readonly className: string = 'Right';
}

export class Reverse extends Func {
  override get key(): string { return 'reverse'; }
  static readonly className: string = 'Reverse';
}

export class Length extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'binary': false, 'encoding': false };
  static readonly sqlNames: readonly string[] = ['LENGTH', 'LEN', 'CHAR_LENGTH', 'CHARACTER_LENGTH'];
  override get key(): string { return 'length'; }
  static readonly className: string = 'Length';
}

export class RtrimmedLength extends Func {
  override get key(): string { return 'rtrimmedlength'; }
  static readonly className: string = 'RtrimmedLength';
}

export class BitLength extends Func {
  override get key(): string { return 'bitlength'; }
  static readonly className: string = 'BitLength';
}

export class Levenshtein extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'ins_cost': false, 'del_cost': false, 'sub_cost': false, 'max_dist': false };
  override get key(): string { return 'levenshtein'; }
  static readonly className: string = 'Levenshtein';
}

export class Ln extends Func {
  override get key(): string { return 'ln'; }
  static readonly className: string = 'Ln';
}

export class Log extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'log'; }
  static readonly className: string = 'Log';
}

export class LogicalOr extends AggFunc {
  static readonly sqlNames: readonly string[] = ['LOGICAL_OR', 'BOOL_OR', 'BOOLOR_AGG'];
  override get key(): string { return 'logicalor'; }
  static readonly className: string = 'LogicalOr';
}

export class LogicalAnd extends AggFunc {
  static readonly sqlNames: readonly string[] = ['LOGICAL_AND', 'BOOL_AND', 'BOOLAND_AGG'];
  override get key(): string { return 'logicaland'; }
  static readonly className: string = 'LogicalAnd';
}

export class Lower extends Func {
  static readonly sqlNames: readonly string[] = ['LOWER', 'LCASE'];
  override get key(): string { return 'lower'; }
  static readonly className: string = 'Lower';
}

export class Map extends Func {
  static readonly argTypes: Record<string, boolean> = { 'keys': false, 'values': false };
  override get key(): string { return 'map'; }
  static readonly className: string = 'Map';
}

export class ToMap extends Func {
  override get key(): string { return 'tomap'; }
  static readonly className: string = 'ToMap';
}

export class MapFromEntries extends Func {
  override get key(): string { return 'mapfromentries'; }
  static readonly className: string = 'MapFromEntries';
}

export class MapCat extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'mapcat'; }
  static readonly className: string = 'MapCat';
}

export class MapContainsKey extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'key': true };
  override get key(): string { return 'mapcontainskey'; }
  static readonly className: string = 'MapContainsKey';
}

export class MapDelete extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'mapdelete'; }
  static readonly className: string = 'MapDelete';
}

export class MapInsert extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'key': false, 'value': true, 'update_flag': false };
  override get key(): string { return 'mapinsert'; }
  static readonly className: string = 'MapInsert';
}

export class MapKeys extends Func {
  override get key(): string { return 'mapkeys'; }
  static readonly className: string = 'MapKeys';
}

export class MapPick extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'mappick'; }
  static readonly className: string = 'MapPick';
}

export class MapSize extends Func {
  override get key(): string { return 'mapsize'; }
  static readonly className: string = 'MapSize';
}

export class ScopeResolution extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expression': true };
  get key(): string { return 'scoperesolution'; }
  static readonly className: string = 'ScopeResolution';
}

export class Slice extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expression': false, 'step': false };
  get key(): string { return 'slice'; }
  static readonly className: string = 'Slice';
}

export class Stream extends Expression {
  get key(): string { return 'stream'; }
  static readonly className: string = 'Stream';
}

export class StarMap extends Func {
  override get key(): string { return 'starmap'; }
  static readonly className: string = 'StarMap';
}

export class VarMap extends Func {
  static readonly argTypes: Record<string, boolean> = { 'keys': true, 'values': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'varmap'; }
  static readonly className: string = 'VarMap';
}

export class MatchAgainst extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'modifier': false };
  override get key(): string { return 'matchagainst'; }
  static readonly className: string = 'MatchAgainst';
}

export class Max extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'max'; }
  static readonly className: string = 'Max';
}

export class MD5 extends Func {
  static readonly sqlNames: readonly string[] = ['MD5'];
  override get key(): string { return 'md5'; }
  static readonly className: string = 'MD5';
}

export class MD5Digest extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['MD5_DIGEST'];
  override get key(): string { return 'md5digest'; }
  static readonly className: string = 'MD5Digest';
}

export class MD5NumberLower64 extends Func {
  override get key(): string { return 'md5numberlower64'; }
  static readonly className: string = 'MD5NumberLower64';
}

export class MD5NumberUpper64 extends Func {
  override get key(): string { return 'md5numberupper64'; }
  static readonly className: string = 'MD5NumberUpper64';
}

export class Median extends AggFunc {
  override get key(): string { return 'median'; }
  static readonly className: string = 'Median';
}

export class Mode extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'deterministic': false };
  override get key(): string { return 'mode'; }
  static readonly className: string = 'Mode';
}

export class Min extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'min'; }
  static readonly className: string = 'Min';
}

export class Month extends Func {
  override get key(): string { return 'month'; }
  static readonly className: string = 'Month';
}

export class Monthname extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'abbreviated': false };
  override get key(): string { return 'monthname'; }
  static readonly className: string = 'Monthname';
}

export class AddMonths extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'preserve_end_of_month': false };
  override get key(): string { return 'addmonths'; }
  static readonly className: string = 'AddMonths';
}

export class Nvl2 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'true': true, 'false': false };
  override get key(): string { return 'nvl2'; }
  static readonly className: string = 'Nvl2';
}

export class Ntile extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'ntile'; }
  static readonly className: string = 'Ntile';
}

export class Normalize extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'form': false, 'is_casefold': false };
  override get key(): string { return 'normalize'; }
  static readonly className: string = 'Normalize';
}

export class Normal extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'stddev': true, 'gen': true };
  override get key(): string { return 'normal'; }
  static readonly className: string = 'Normal';
}

export class NetFunc extends Func {
  override get key(): string { return 'netfunc'; }
  static readonly className: string = 'NetFunc';
}

export class Host extends Func {
  override get key(): string { return 'host'; }
  static readonly className: string = 'Host';
}

export class RegDomain extends Func {
  override get key(): string { return 'regdomain'; }
  static readonly className: string = 'RegDomain';
}

export class Overlay extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'from_': true, 'for_': false };
  override get key(): string { return 'overlay'; }
  static readonly className: string = 'Overlay';
}

export class Predict extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'params_struct': false };
  override get key(): string { return 'predict'; }
  static readonly className: string = 'Predict';
}

export class MLTranslate extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'params_struct': true };
  override get key(): string { return 'mltranslate'; }
  static readonly className: string = 'MLTranslate';
}

export class FeaturesAtTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'time': false, 'num_rows': false, 'ignore_feature_nulls': false };
  override get key(): string { return 'featuresattime'; }
  static readonly className: string = 'FeaturesAtTime';
}

export class GenerateEmbedding extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'params_struct': false, 'is_text': false };
  override get key(): string { return 'generateembedding'; }
  static readonly className: string = 'GenerateEmbedding';
}

export class MLForecast extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'params_struct': false };
  override get key(): string { return 'mlforecast'; }
  static readonly className: string = 'MLForecast';
}

export class ModelAttribute extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  get key(): string { return 'modelattribute'; }
  static readonly className: string = 'ModelAttribute';
}

export class VectorSearch extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'column_to_search': true, 'query_table': true, 'query_column_to_search': false, 'top_k': false, 'distance_type': false, 'options': false };
  override get key(): string { return 'vectorsearch'; }
  static readonly className: string = 'VectorSearch';
}

export class Pi extends Func {
  static readonly argTypes: Record<string, boolean> = {};
  override get key(): string { return 'pi'; }
  static readonly className: string = 'Pi';
}

// Also extends: Func
export class Pow extends Binary {
  static readonly sqlNames: readonly string[] = ['POWER', 'POW'];
  override get key(): string { return 'pow'; }
  static readonly className: string = 'Pow';
  override get name(): string {
    const ctor = this.constructor as typeof Pow;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class PercentileCont extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'percentilecont'; }
  static readonly className: string = 'PercentileCont';
}

export class PercentileDisc extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'percentiledisc'; }
  static readonly className: string = 'PercentileDisc';
}

export class PercentRank extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'percentrank'; }
  static readonly className: string = 'PercentRank';
}

export class Quantile extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'quantile': true };
  override get key(): string { return 'quantile'; }
  static readonly className: string = 'Quantile';
}

export class ApproxQuantile extends Quantile {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'quantile': true, 'accuracy': false, 'weight': false, 'error_tolerance': false };
  override get key(): string { return 'approxquantile'; }
  static readonly className: string = 'ApproxQuantile';
}

export class ApproxPercentileAccumulate extends AggFunc {
  override get key(): string { return 'approxpercentileaccumulate'; }
  static readonly className: string = 'ApproxPercentileAccumulate';
}

export class ApproxPercentileEstimate extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'percentile': true };
  override get key(): string { return 'approxpercentileestimate'; }
  static readonly className: string = 'ApproxPercentileEstimate';
}

export class Quarter extends Func {
  override get key(): string { return 'quarter'; }
  static readonly className: string = 'Quarter';
}

export class Rand extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'lower': false, 'upper': false };
  static readonly sqlNames: readonly string[] = ['RAND', 'RANDOM'];
  override get key(): string { return 'rand'; }
  static readonly className: string = 'Rand';
}

export class Randn extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'randn'; }
  static readonly className: string = 'Randn';
}

export class Randstr extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'generator': false };
  override get key(): string { return 'randstr'; }
  static readonly className: string = 'Randstr';
}

export class RangeN extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': true, 'each': false };
  override get key(): string { return 'rangen'; }
  static readonly className: string = 'RangeN';
}

export class RangeBucket extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'rangebucket'; }
  static readonly className: string = 'RangeBucket';
}

export class Rank extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'rank'; }
  static readonly className: string = 'Rank';
}

export class ReadCSV extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false };
  static readonly isVarLenArgs = true;
  static readonly sqlNames: readonly string[] = ['READ_CSV'];
  override get key(): string { return 'readcsv'; }
  static readonly className: string = 'ReadCSV';
}

export class ReadParquet extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'readparquet'; }
  static readonly className: string = 'ReadParquet';
}

export class Reduce extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'initial': true, 'merge': true, 'finish': false };
  override get key(): string { return 'reduce'; }
  static readonly className: string = 'Reduce';
}

export class RegexpExtract extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'position': false, 'occurrence': false, 'parameters': false, 'group': false, 'null_if_pos_overflow': false };
  override get key(): string { return 'regexpextract'; }
  static readonly className: string = 'RegexpExtract';
}

export class RegexpExtractAll extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'group': false, 'parameters': false, 'position': false, 'occurrence': false };
  override get key(): string { return 'regexpextractall'; }
  static readonly className: string = 'RegexpExtractAll';
}

export class RegexpReplace extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'replacement': false, 'position': false, 'occurrence': false, 'modifiers': false, 'single_replace': false };
  override get key(): string { return 'regexpreplace'; }
  static readonly className: string = 'RegexpReplace';
}

// Also extends: Func
export class RegexpLike extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'flag': false };
  override get key(): string { return 'regexplike'; }
  static readonly className: string = 'RegexpLike';
  override get name(): string {
    const ctor = this.constructor as typeof RegexpLike;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class RegexpILike extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'flag': false };
  override get key(): string { return 'regexpilike'; }
  static readonly className: string = 'RegexpILike';
  override get name(): string {
    const ctor = this.constructor as typeof RegexpILike;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

// Also extends: Func
export class RegexpFullMatch extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'options': false };
  override get key(): string { return 'regexpfullmatch'; }
  static readonly className: string = 'RegexpFullMatch';
  override get name(): string {
    const ctor = this.constructor as typeof RegexpFullMatch;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class RegexpInstr extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'position': false, 'occurrence': false, 'option': false, 'parameters': false, 'group': false };
  override get key(): string { return 'regexpinstr'; }
  static readonly className: string = 'RegexpInstr';
}

export class RegexpSplit extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'limit': false };
  override get key(): string { return 'regexpsplit'; }
  static readonly className: string = 'RegexpSplit';
}

export class RegexpCount extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'position': false, 'parameters': false };
  override get key(): string { return 'regexpcount'; }
  static readonly className: string = 'RegexpCount';
}

export class RegrValx extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrvalx'; }
  static readonly className: string = 'RegrValx';
}

export class RegrValy extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrvaly'; }
  static readonly className: string = 'RegrValy';
}

export class RegrAvgy extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regravgy'; }
  static readonly className: string = 'RegrAvgy';
}

export class RegrAvgx extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regravgx'; }
  static readonly className: string = 'RegrAvgx';
}

export class RegrCount extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrcount'; }
  static readonly className: string = 'RegrCount';
}

export class RegrIntercept extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrintercept'; }
  static readonly className: string = 'RegrIntercept';
}

export class RegrR2 extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrr2'; }
  static readonly className: string = 'RegrR2';
}

export class RegrSxx extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrsxx'; }
  static readonly className: string = 'RegrSxx';
}

export class RegrSxy extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrsxy'; }
  static readonly className: string = 'RegrSxy';
}

export class RegrSyy extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrsyy'; }
  static readonly className: string = 'RegrSyy';
}

export class RegrSlope extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'regrslope'; }
  static readonly className: string = 'RegrSlope';
}

export class Repeat extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'times': true };
  override get key(): string { return 'repeat'; }
  static readonly className: string = 'Repeat';
}

export class Replace extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'replacement': false };
  override get key(): string { return 'replace'; }
  static readonly className: string = 'Replace';
}

export class Radians extends Func {
  override get key(): string { return 'radians'; }
  static readonly className: string = 'Radians';
}

export class Round extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'decimals': false, 'truncate': false, 'casts_non_integer_decimals': false };
  override get key(): string { return 'round'; }
  static readonly className: string = 'Round';
}

export class RowNumber extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'rownumber'; }
  static readonly className: string = 'RowNumber';
}

export class Seq1 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'seq1'; }
  static readonly className: string = 'Seq1';
}

export class Seq2 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'seq2'; }
  static readonly className: string = 'Seq2';
}

export class Seq4 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'seq4'; }
  static readonly className: string = 'Seq4';
}

export class Seq8 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false };
  override get key(): string { return 'seq8'; }
  static readonly className: string = 'Seq8';
}

export class SafeAdd extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'safeadd'; }
  static readonly className: string = 'SafeAdd';
}

export class SafeDivide extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'safedivide'; }
  static readonly className: string = 'SafeDivide';
}

export class SafeMultiply extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'safemultiply'; }
  static readonly className: string = 'SafeMultiply';
}

export class SafeNegate extends Func {
  override get key(): string { return 'safenegate'; }
  static readonly className: string = 'SafeNegate';
}

export class SafeSubtract extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'safesubtract'; }
  static readonly className: string = 'SafeSubtract';
}

export class SafeConvertBytesToString extends Func {
  override get key(): string { return 'safeconvertbytestostring'; }
  static readonly className: string = 'SafeConvertBytesToString';
}

export class SHA extends Func {
  static readonly sqlNames: readonly string[] = ['SHA', 'SHA1'];
  override get key(): string { return 'sha'; }
  static readonly className: string = 'SHA';
}

export class SHA2 extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'length': false };
  static readonly sqlNames: readonly string[] = ['SHA2'];
  override get key(): string { return 'sha2'; }
  static readonly className: string = 'SHA2';
}

export class SHA1Digest extends Func {
  override get key(): string { return 'sha1digest'; }
  static readonly className: string = 'SHA1Digest';
}

export class SHA2Digest extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'length': false };
  override get key(): string { return 'sha2digest'; }
  static readonly className: string = 'SHA2Digest';
}

export class Sign extends Func {
  static readonly sqlNames: readonly string[] = ['SIGN', 'SIGNUM'];
  override get key(): string { return 'sign'; }
  static readonly className: string = 'Sign';
}

export class SortArray extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'asc': false, 'nulls_first': false };
  override get key(): string { return 'sortarray'; }
  static readonly className: string = 'SortArray';
}

export class Soundex extends Func {
  override get key(): string { return 'soundex'; }
  static readonly className: string = 'Soundex';
}

export class SoundexP123 extends Func {
  override get key(): string { return 'soundexp123'; }
  static readonly className: string = 'SoundexP123';
}

export class Split extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'limit': false };
  override get key(): string { return 'split'; }
  static readonly className: string = 'Split';
}

export class SplitPart extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'delimiter': false, 'part_index': false };
  override get key(): string { return 'splitpart'; }
  static readonly className: string = 'SplitPart';
}

export class Substring extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'start': false, 'length': false };
  static readonly sqlNames: readonly string[] = ['SUBSTRING', 'SUBSTR'];
  override get key(): string { return 'substring'; }
  static readonly className: string = 'Substring';
}

export class SubstringIndex extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'delimiter': true, 'count': true };
  override get key(): string { return 'substringindex'; }
  static readonly className: string = 'SubstringIndex';
}

export class StandardHash extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'standardhash'; }
  static readonly className: string = 'StandardHash';
}

export class StartsWith extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  static readonly sqlNames: readonly string[] = ['STARTS_WITH', 'STARTSWITH'];
  override get key(): string { return 'startswith'; }
  static readonly className: string = 'StartsWith';
}

export class EndsWith extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  static readonly sqlNames: readonly string[] = ['ENDS_WITH', 'ENDSWITH'];
  override get key(): string { return 'endswith'; }
  static readonly className: string = 'EndsWith';
}

export class StrPosition extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'substr': true, 'position': false, 'occurrence': false };
  override get key(): string { return 'strposition'; }
  static readonly className: string = 'StrPosition';
}

export class Search extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'json_scope': false, 'analyzer': false, 'analyzer_options': false, 'search_mode': false };
  override get key(): string { return 'search'; }
  static readonly className: string = 'Search';
}

export class SearchIp extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'searchip'; }
  static readonly className: string = 'SearchIp';
}

export class StrToDate extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'safe': false };
  override get key(): string { return 'strtodate'; }
  static readonly className: string = 'StrToDate';
}

export class StrToTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': true, 'zone': false, 'safe': false, 'target_type': false };
  override get key(): string { return 'strtotime'; }
  static readonly className: string = 'StrToTime';
}

export class StrToUnix extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'format': false };
  override get key(): string { return 'strtounix'; }
  static readonly className: string = 'StrToUnix';
}

export class StrToMap extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'pair_delim': false, 'key_value_delim': false, 'duplicate_resolution_callback': false };
  override get key(): string { return 'strtomap'; }
  static readonly className: string = 'StrToMap';
}

export class NumberToStr extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': true, 'culture': false };
  override get key(): string { return 'numbertostr'; }
  static readonly className: string = 'NumberToStr';
}

export class FromBase extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'frombase'; }
  static readonly className: string = 'FromBase';
}

export class Space extends Func {
  override get key(): string { return 'space'; }
  static readonly className: string = 'Space';
}

export class Struct extends Func {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'struct'; }
  static readonly className: string = 'Struct';
}

export class StructExtract extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'structextract'; }
  static readonly className: string = 'StructExtract';
}

export class Stuff extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'start': true, 'length': true, 'expression': true };
  static readonly sqlNames: readonly string[] = ['STUFF', 'INSERT'];
  override get key(): string { return 'stuff'; }
  static readonly className: string = 'Stuff';
}

export class Sum extends AggFunc {
  override get key(): string { return 'sum'; }
  static readonly className: string = 'Sum';
}

export class Sqrt extends Func {
  override get key(): string { return 'sqrt'; }
  static readonly className: string = 'Sqrt';
}

export class Stddev extends AggFunc {
  static readonly sqlNames: readonly string[] = ['STDDEV', 'STDEV'];
  override get key(): string { return 'stddev'; }
  static readonly className: string = 'Stddev';
}

export class StddevPop extends AggFunc {
  override get key(): string { return 'stddevpop'; }
  static readonly className: string = 'StddevPop';
}

export class StddevSamp extends AggFunc {
  override get key(): string { return 'stddevsamp'; }
  static readonly className: string = 'StddevSamp';
}

export class Time extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'zone': false };
  override get key(): string { return 'time'; }
  static readonly className: string = 'Time';
}

export class TimeToStr extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': true, 'culture': false, 'zone': false };
  override get key(): string { return 'timetostr'; }
  static readonly className: string = 'TimeToStr';
}

export class TimeToTimeStr extends Func {
  override get key(): string { return 'timetotimestr'; }
  static readonly className: string = 'TimeToTimeStr';
}

export class TimeToUnix extends Func {
  override get key(): string { return 'timetounix'; }
  static readonly className: string = 'TimeToUnix';
}

export class TimeStrToDate extends Func {
  override get key(): string { return 'timestrtodate'; }
  static readonly className: string = 'TimeStrToDate';
}

export class TimeStrToTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'zone': false };
  override get key(): string { return 'timestrtotime'; }
  static readonly className: string = 'TimeStrToTime';
}

export class TimeStrToUnix extends Func {
  override get key(): string { return 'timestrtounix'; }
  static readonly className: string = 'TimeStrToUnix';
}

export class Trim extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false, 'position': false, 'collation': false };
  override get key(): string { return 'trim'; }
  static readonly className: string = 'Trim';
}

// Also extends: TimeUnit
export class TsOrDsAdd extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false, 'return_type': false };
  override get key(): string { return 'tsordsadd'; }
  static readonly className: string = 'TsOrDsAdd';
}

// Also extends: TimeUnit
export class TsOrDsDiff extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'unit': false };
  override get key(): string { return 'tsordsdiff'; }
  static readonly className: string = 'TsOrDsDiff';
}

export class TsOrDsToDateStr extends Func {
  override get key(): string { return 'tsordstodatestr'; }
  static readonly className: string = 'TsOrDsToDateStr';
}

export class TsOrDsToDate extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'safe': false };
  override get key(): string { return 'tsordstodate'; }
  static readonly className: string = 'TsOrDsToDate';
}

export class TsOrDsToDatetime extends Func {
  override get key(): string { return 'tsordstodatetime'; }
  static readonly className: string = 'TsOrDsToDatetime';
}

export class TsOrDsToTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false, 'safe': false };
  override get key(): string { return 'tsordstotime'; }
  static readonly className: string = 'TsOrDsToTime';
}

export class TsOrDsToTimestamp extends Func {
  override get key(): string { return 'tsordstotimestamp'; }
  static readonly className: string = 'TsOrDsToTimestamp';
}

export class TsOrDiToDi extends Func {
  override get key(): string { return 'tsorditodi'; }
  static readonly className: string = 'TsOrDiToDi';
}

export class Unhex extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  override get key(): string { return 'unhex'; }
  static readonly className: string = 'Unhex';
}

export class Unicode extends Func {
  override get key(): string { return 'unicode'; }
  static readonly className: string = 'Unicode';
}

export class Uniform extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'gen': false, 'seed': false };
  override get key(): string { return 'uniform'; }
  static readonly className: string = 'Uniform';
}

export class UnixDate extends Func {
  override get key(): string { return 'unixdate'; }
  static readonly className: string = 'UnixDate';
}

export class UnixToStr extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'format': false };
  override get key(): string { return 'unixtostr'; }
  static readonly className: string = 'UnixToStr';
}

export class UnixToTime extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'scale': false, 'zone': false, 'hours': false, 'minutes': false, 'format': false, 'target_type': false };
  override get key(): string { return 'unixtotime'; }
  static readonly className: string = 'UnixToTime';
}

export class UnixToTimeStr extends Func {
  override get key(): string { return 'unixtotimestr'; }
  static readonly className: string = 'UnixToTimeStr';
}

export class UnixSeconds extends Func {
  override get key(): string { return 'unixseconds'; }
  static readonly className: string = 'UnixSeconds';
}

export class UnixMicros extends Func {
  override get key(): string { return 'unixmicros'; }
  static readonly className: string = 'UnixMicros';
}

export class UnixMillis extends Func {
  override get key(): string { return 'unixmillis'; }
  static readonly className: string = 'UnixMillis';
}

export class Uuid extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'name': false, 'is_string': false };
  static readonly sqlNames: readonly string[] = ['UUID', 'GEN_RANDOM_UUID', 'GENERATE_UUID', 'UUID_STRING'];
  override get key(): string { return 'uuid'; }
  static readonly className: string = 'Uuid';
}

export class TimestampFromParts extends Func {
  static readonly argTypes: Record<string, boolean> = { 'year': false, 'month': false, 'day': false, 'hour': false, 'min': false, 'sec': false, 'nano': false, 'zone': false, 'milli': false, 'this': false, 'expression': false };
  static readonly sqlNames: readonly string[] = ['TIMESTAMP_FROM_PARTS', 'TIMESTAMPFROMPARTS'];
  override get key(): string { return 'timestampfromparts'; }
  static readonly className: string = 'TimestampFromParts';
}

export class TimestampLtzFromParts extends Func {
  static readonly argTypes: Record<string, boolean> = { 'year': false, 'month': false, 'day': false, 'hour': false, 'min': false, 'sec': false, 'nano': false };
  static readonly sqlNames: readonly string[] = ['TIMESTAMP_LTZ_FROM_PARTS', 'TIMESTAMPLTZFROMPARTS'];
  override get key(): string { return 'timestampltzfromparts'; }
  static readonly className: string = 'TimestampLtzFromParts';
}

export class TimestampTzFromParts extends Func {
  static readonly argTypes: Record<string, boolean> = { 'year': false, 'month': false, 'day': false, 'hour': false, 'min': false, 'sec': false, 'nano': false, 'zone': false };
  static readonly sqlNames: readonly string[] = ['TIMESTAMP_TZ_FROM_PARTS', 'TIMESTAMPTZFROMPARTS'];
  override get key(): string { return 'timestamptzfromparts'; }
  static readonly className: string = 'TimestampTzFromParts';
}

export class Upper extends Func {
  static readonly sqlNames: readonly string[] = ['UPPER', 'UCASE'];
  override get key(): string { return 'upper'; }
  static readonly className: string = 'Upper';
}

// Also extends: AggFunc
export class Corr extends Binary {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'null_on_zero_variance': false };
  override get key(): string { return 'corr'; }
  static readonly className: string = 'Corr';
  override get name(): string {
    const ctor = this.constructor as typeof Corr;
    const first = (ctor as any).sqlNames?.[0];
    if (first) return first;
    return camelToSnakeCase(ctor.className);
  }
}

export class CumeDist extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'expressions': false };
  static readonly isVarLenArgs = true;
  override get key(): string { return 'cumedist'; }
  static readonly className: string = 'CumeDist';
}

export class Variance extends AggFunc {
  static readonly sqlNames: readonly string[] = ['VARIANCE', 'VARIANCE_SAMP', 'VAR_SAMP'];
  override get key(): string { return 'variance'; }
  static readonly className: string = 'Variance';
}

export class VariancePop extends AggFunc {
  static readonly sqlNames: readonly string[] = ['VARIANCE_POP', 'VAR_POP'];
  override get key(): string { return 'variancepop'; }
  static readonly className: string = 'VariancePop';
}

export class Kurtosis extends AggFunc {
  override get key(): string { return 'kurtosis'; }
  static readonly className: string = 'Kurtosis';
}

export class Skewness extends AggFunc {
  override get key(): string { return 'skewness'; }
  static readonly className: string = 'Skewness';
}

export class WidthBucket extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'min_value': false, 'max_value': false, 'num_buckets': false, 'threshold': false };
  override get key(): string { return 'widthbucket'; }
  static readonly className: string = 'WidthBucket';
}

export class CovarSamp extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'covarsamp'; }
  static readonly className: string = 'CovarSamp';
}

export class CovarPop extends AggFunc {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'covarpop'; }
  static readonly className: string = 'CovarPop';
}

export class Week extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'mode': false };
  override get key(): string { return 'week'; }
  static readonly className: string = 'Week';
}

export class WeekStart extends Expression {
  get key(): string { return 'weekstart'; }
  static readonly className: string = 'WeekStart';
}

export class NextDay extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true };
  override get key(): string { return 'nextday'; }
  static readonly className: string = 'NextDay';
}

export class XMLElement extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expressions': false, 'evalname': false };
  static readonly sqlNames: readonly string[] = ['XMLELEMENT'];
  override get key(): string { return 'xmlelement'; }
  static readonly className: string = 'XMLElement';
}

export class XMLGet extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': true, 'instance': false };
  static readonly sqlNames: readonly string[] = ['XMLGET'];
  override get key(): string { return 'xmlget'; }
  static readonly className: string = 'XMLGet';
}

export class XMLTable extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'namespaces': false, 'passing': false, 'columns': false, 'by_ref': false };
  override get key(): string { return 'xmltable'; }
  static readonly className: string = 'XMLTable';
}

export class XMLNamespace extends Expression {
  get key(): string { return 'xmlnamespace'; }
  static readonly className: string = 'XMLNamespace';
}

export class XMLKeyValueOption extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'expression': false };
  get key(): string { return 'xmlkeyvalueoption'; }
  static readonly className: string = 'XMLKeyValueOption';
}

export class Year extends Func {
  override get key(): string { return 'year'; }
  static readonly className: string = 'Year';
}

export class Zipf extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'elementcount': true, 'gen': true };
  override get key(): string { return 'zipf'; }
  static readonly className: string = 'Zipf';
}

export class Use extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'this': false, 'expressions': false, 'kind': false };
  get key(): string { return 'use'; }
  static readonly className: string = 'Use';
}

export class Merge extends DML {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'using': true, 'on': false, 'using_cond': false, 'whens': true, 'with_': false, 'returning': false };
  override get key(): string { return 'merge'; }
  static readonly className: string = 'Merge';
}

export class When extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'matched': true, 'source': false, 'condition': false, 'then': true };
  get key(): string { return 'when'; }
  static readonly className: string = 'When';
}

export class Whens extends Expression {
  static readonly argTypes: Record<string, boolean> = { 'expressions': true };
  get key(): string { return 'whens'; }
  static readonly className: string = 'Whens';
}

export class NextValueFor extends Func {
  static readonly argTypes: Record<string, boolean> = { 'this': true, 'order': false };
  override get key(): string { return 'nextvaluefor'; }
  static readonly className: string = 'NextValueFor';
}

export class Semicolon extends Expression {
  static readonly argTypes: Record<string, boolean> = {};
  get key(): string { return 'semicolon'; }
  static readonly className: string = 'Semicolon';
}

export class TableColumn extends Expression {
  get key(): string { return 'tablecolumn'; }
  static readonly className: string = 'TableColumn';
}

export class Variadic extends Expression {
  get key(): string { return 'variadic'; }
  static readonly className: string = 'Variadic';
}

type NamedExpressionClass = ExpressionClass & { readonly className: string };

export const GENERATED_CLASSES: readonly NamedExpressionClass[] = [
  Condition,
  Predicate,
  DerivedTable,
  Query,
  UDTF,
  Cache,
  Uncache,
  Refresh,
  DDL,
  LockingStatement,
  DML,
  Create,
  SequenceProperties,
  TruncateTable,
  Clone,
  Describe,
  Attach,
  Detach,
  Install,
  Summarize,
  Kill,
  Pragma,
  Declare,
  DeclareItem,
  Set,
  Heredoc,
  SetItem,
  QueryBand,
  Show,
  UserDefinedFunction,
  CharacterSet,
  RecursiveWithSearch,
  With,
  WithinGroup,
  CTE,
  ProjectionDef,
  TableAlias,
  BitString,
  HexString,
  ByteString,
  RawString,
  UnicodeString,
  Column,
  Pseudocolumn,
  ColumnPosition,
  ColumnDef,
  AlterColumn,
  AlterIndex,
  AlterDistStyle,
  AlterSortKey,
  AlterSet,
  RenameColumn,
  AlterRename,
  SwapTable,
  Comment,
  Comprehension,
  MergeTreeTTLAction,
  MergeTreeTTL,
  IndexConstraintOption,
  ColumnConstraint,
  ColumnConstraintKind,
  AutoIncrementColumnConstraint,
  ZeroFillColumnConstraint,
  PeriodForSystemTimeConstraint,
  CaseSpecificColumnConstraint,
  CharacterSetColumnConstraint,
  CheckColumnConstraint,
  ClusteredColumnConstraint,
  CollateColumnConstraint,
  CommentColumnConstraint,
  CompressColumnConstraint,
  DateFormatColumnConstraint,
  DefaultColumnConstraint,
  EncodeColumnConstraint,
  ExcludeColumnConstraint,
  EphemeralColumnConstraint,
  WithOperator,
  GeneratedAsIdentityColumnConstraint,
  GeneratedAsRowColumnConstraint,
  IndexColumnConstraint,
  InlineLengthColumnConstraint,
  NonClusteredColumnConstraint,
  NotForReplicationColumnConstraint,
  MaskingPolicyColumnConstraint,
  NotNullColumnConstraint,
  OnUpdateColumnConstraint,
  PrimaryKeyColumnConstraint,
  TitleColumnConstraint,
  UniqueColumnConstraint,
  UppercaseColumnConstraint,
  WatermarkColumnConstraint,
  PathColumnConstraint,
  ProjectionPolicyColumnConstraint,
  ComputedColumnConstraint,
  InOutColumnConstraint,
  Constraint,
  Delete,
  Drop,
  Export,
  Filter,
  Check,
  Changes,
  Connect,
  CopyParameter,
  Copy,
  Credentials,
  Prior,
  Directory,
  DirectoryStage,
  ForeignKey,
  ColumnPrefix,
  PrimaryKey,
  Into,
  From,
  Having,
  Hint,
  JoinHint,
  Identifier,
  Opclass,
  Index,
  IndexParameters,
  Insert,
  ConditionalInsert,
  MultitableInserts,
  OnConflict,
  OnCondition,
  Returning,
  Introducer,
  National,
  LoadData,
  Partition,
  PartitionRange,
  PartitionId,
  Fetch,
  Grant,
  Revoke,
  Group,
  Cube,
  Rollup,
  GroupingSets,
  Lambda,
  Limit,
  LimitOptions,
  Literal,
  Join,
  Lateral,
  TableFromRows,
  MatchRecognizeMeasure,
  MatchRecognize,
  Final,
  Offset,
  Order,
  WithFill,
  Cluster,
  Distribute,
  Sort,
  Ordered,
  Property,
  GrantPrivilege,
  GrantPrincipal,
  AllowedValuesProperty,
  AlgorithmProperty,
  AutoIncrementProperty,
  AutoRefreshProperty,
  BackupProperty,
  BuildProperty,
  BlockCompressionProperty,
  CharacterSetProperty,
  ChecksumProperty,
  CollateProperty,
  CopyGrantsProperty,
  DataBlocksizeProperty,
  DataDeletionProperty,
  DefinerProperty,
  DistKeyProperty,
  DistributedByProperty,
  DistStyleProperty,
  DuplicateKeyProperty,
  EngineProperty,
  HeapProperty,
  ToTableProperty,
  ExecuteAsProperty,
  ExternalProperty,
  FallbackProperty,
  FileFormatProperty,
  CredentialsProperty,
  FreespaceProperty,
  GlobalProperty,
  IcebergProperty,
  InheritsProperty,
  InputModelProperty,
  OutputModelProperty,
  IsolatedLoadingProperty,
  JournalProperty,
  LanguageProperty,
  EnviromentProperty,
  ClusteredByProperty,
  DictProperty,
  DictSubProperty,
  DictRange,
  DynamicProperty,
  OnCluster,
  EmptyProperty,
  LikeProperty,
  LocationProperty,
  LockProperty,
  LockingProperty,
  LogProperty,
  MaterializedProperty,
  MergeBlockRatioProperty,
  NoPrimaryIndexProperty,
  OnProperty,
  OnCommitProperty,
  PartitionedByProperty,
  PartitionedByBucket,
  PartitionByTruncate,
  PartitionByRangeProperty,
  PartitionByRangePropertyDynamic,
  RollupProperty,
  RollupIndex,
  PartitionByListProperty,
  PartitionList,
  RefreshTriggerProperty,
  UniqueKeyProperty,
  PartitionBoundSpec,
  PartitionedOfProperty,
  StreamingTableProperty,
  RemoteWithConnectionModelProperty,
  ReturnsProperty,
  StrictProperty,
  RowFormatProperty,
  RowFormatDelimitedProperty,
  RowFormatSerdeProperty,
  QueryTransform,
  SampleProperty,
  SecurityProperty,
  SchemaCommentProperty,
  SemanticView,
  SerdeProperties,
  SetProperty,
  SharingProperty,
  SetConfigProperty,
  SettingsProperty,
  SortKeyProperty,
  SqlReadWriteProperty,
  SqlSecurityProperty,
  StabilityProperty,
  StorageHandlerProperty,
  TemporaryProperty,
  SecureProperty,
  Tags,
  TransformModelProperty,
  TransientProperty,
  UnloggedProperty,
  UsingTemplateProperty,
  ViewAttributeProperty,
  VolatileProperty,
  WithDataProperty,
  WithJournalTableProperty,
  WithSchemaBindingProperty,
  WithSystemVersioningProperty,
  WithProcedureOptions,
  EncodeProperty,
  IncludeProperty,
  ForceProperty,
  Properties,
  Qualify,
  InputOutputFormat,
  Return,
  Reference,
  Tuple,
  QueryOption,
  WithTableHint,
  IndexTableHint,
  HistoricalData,
  Put,
  Get,
  Table,
  SetOperation,
  Union,
  Except,
  Intersect,
  Update,
  Values,
  Var,
  Version,
  Schema,
  Lock,
  Select,
  Subquery,
  TableSample,
  Tag,
  Pivot,
  UnpivotColumns,
  Window,
  WindowSpec,
  PreWhere,
  Where,
  Star,
  Parameter,
  SessionParameter,
  Placeholder,
  Null,
  Boolean,
  DataTypeParam,
  DataType,
  PseudoType,
  ObjectIdentifier,
  SubqueryPredicate,
  All,
  Any,
  Command,
  Transaction,
  Commit,
  Rollback,
  Alter,
  AlterSession,
  Analyze,
  AnalyzeStatistics,
  AnalyzeHistogram,
  AnalyzeSample,
  AnalyzeListChainedRows,
  AnalyzeDelete,
  AnalyzeWith,
  AnalyzeValidate,
  AnalyzeColumns,
  UsingData,
  AddConstraint,
  AddPartition,
  AttachOption,
  DropPartition,
  ReplacePartition,
  Binary,
  Add,
  Connector,
  BitwiseAnd,
  BitwiseLeftShift,
  BitwiseOr,
  BitwiseRightShift,
  BitwiseXor,
  Div,
  Overlaps,
  ExtendsLeft,
  ExtendsRight,
  Dot,
  DPipe,
  EQ,
  NullSafeEQ,
  NullSafeNEQ,
  PropertyEQ,
  Distance,
  Escape,
  Glob,
  GT,
  GTE,
  ILike,
  IntDiv,
  Is,
  Kwarg,
  Like,
  Match,
  LT,
  LTE,
  Mod,
  Mul,
  NEQ,
  Operator,
  SimilarTo,
  Sub,
  Adjacent,
  Unary,
  BitwiseNot,
  Not,
  Paren,
  Neg,
  Alias,
  PivotAlias,
  PivotAny,
  Aliases,
  AtIndex,
  AtTimeZone,
  FromTimeZone,
  FormatPhrase,
  Between,
  Bracket,
  Distinct,
  In,
  ForIn,
  TimeUnit,
  IntervalOp,
  IntervalSpan,
  Interval,
  IgnoreNulls,
  RespectNulls,
  HavingMax,
  Func,
  SafeFunc,
  Typeof,
  Acos,
  Acosh,
  Asin,
  Asinh,
  Atan,
  Atanh,
  Atan2,
  Cot,
  Coth,
  Cos,
  Csc,
  Csch,
  Sec,
  Sech,
  Sin,
  Sinh,
  Tan,
  Tanh,
  Degrees,
  Cosh,
  CosineDistance,
  DotProduct,
  EuclideanDistance,
  ManhattanDistance,
  JarowinklerSimilarity,
  AggFunc,
  BitwiseAndAgg,
  BitwiseOrAgg,
  BitwiseXorAgg,
  BoolxorAgg,
  BitwiseCount,
  BitmapBucketNumber,
  BitmapCount,
  BitmapBitPosition,
  BitmapConstructAgg,
  BitmapOrAgg,
  ByteLength,
  Boolnot,
  Booland,
  Boolor,
  JSONBool,
  ArrayRemove,
  ParameterizedAgg,
  Abs,
  ArgMax,
  ArgMin,
  ApproxTopK,
  ApproxTopKAccumulate,
  ApproxTopKCombine,
  ApproxTopKEstimate,
  ApproxTopSum,
  ApproxQuantiles,
  ApproxPercentileCombine,
  Minhash,
  MinhashCombine,
  ApproximateSimilarity,
  FarmFingerprint,
  Flatten,
  Float64,
  Transform,
  Translate,
  Grouping,
  GroupingId,
  Anonymous,
  AnonymousAggFunc,
  CombinedAggFunc,
  CombinedParameterizedAgg,
  HashAgg,
  Hll,
  ApproxDistinct,
  Apply,
  Array,
  Ascii,
  ToArray,
  ToBoolean,
  List,
  Pad,
  ToChar,
  ToCodePoints,
  ToNumber,
  ToDouble,
  ToDecfloat,
  TryToDecfloat,
  ToFile,
  CodePointsToBytes,
  Columns,
  Convert,
  ConvertToCharset,
  ConvertTimezone,
  CodePointsToString,
  GenerateSeries,
  ExplodingGenerateSeries,
  Generator,
  ArrayAgg,
  ArrayUniqueAgg,
  AIAgg,
  AISummarizeAgg,
  AIClassify,
  ArrayAll,
  ArrayAny,
  ArrayAppend,
  ArrayPrepend,
  ArrayConcat,
  ArrayConcatAgg,
  ArrayCompact,
  ArrayInsert,
  ArrayRemoveAt,
  ArrayConstructCompact,
  ArrayContains,
  ArrayContainsAll,
  ArrayFilter,
  ArrayFirst,
  ArrayLast,
  ArrayReverse,
  ArraySlice,
  ArrayToString,
  ArrayIntersect,
  StPoint,
  StDistance,
  String,
  StringToArray,
  ArrayOverlaps,
  ArraySize,
  ArraySort,
  ArraySum,
  ArrayUnionAgg,
  ArraysZip,
  Avg,
  AnyValue,
  Lag,
  Lead,
  First,
  Last,
  FirstValue,
  LastValue,
  NthValue,
  ObjectAgg,
  Case,
  Cast,
  TryCast,
  JSONCast,
  JustifyDays,
  JustifyHours,
  JustifyInterval,
  Try,
  CastToStrType,
  CheckJson,
  CheckXml,
  TranslateCharacters,
  Collate,
  Collation,
  Ceil,
  Coalesce,
  Chr,
  Concat,
  ConcatWs,
  Contains,
  ConnectByRoot,
  Count,
  CountIf,
  Cbrt,
  CurrentAccount,
  CurrentAccountName,
  CurrentAvailableRoles,
  CurrentClient,
  CurrentIpAddress,
  CurrentDatabase,
  CurrentSchemas,
  CurrentSecondaryRoles,
  CurrentSession,
  CurrentStatement,
  CurrentVersion,
  CurrentTransaction,
  CurrentWarehouse,
  CurrentDate,
  CurrentDatetime,
  CurrentTime,
  Localtime,
  Localtimestamp,
  Systimestamp,
  CurrentTimestamp,
  CurrentTimestampLTZ,
  CurrentTimezone,
  CurrentOrganizationName,
  CurrentSchema,
  CurrentUser,
  CurrentCatalog,
  CurrentRegion,
  CurrentRole,
  CurrentRoleType,
  CurrentOrganizationUser,
  SessionUser,
  UtcDate,
  UtcTime,
  UtcTimestamp,
  DateAdd,
  DateBin,
  DateSub,
  DateDiff,
  DateTrunc,
  Datetime,
  DatetimeAdd,
  DatetimeSub,
  DatetimeDiff,
  DatetimeTrunc,
  DateFromUnixDate,
  DayOfWeek,
  DayOfWeekIso,
  DayOfMonth,
  DayOfYear,
  Dayname,
  ToDays,
  WeekOfYear,
  YearOfWeek,
  YearOfWeekIso,
  MonthsBetween,
  MakeInterval,
  LastDay,
  PreviousDay,
  LaxBool,
  LaxFloat64,
  LaxInt64,
  LaxString,
  Extract,
  Exists,
  Elt,
  Timestamp,
  TimestampAdd,
  TimestampSub,
  TimestampDiff,
  TimestampTrunc,
  TimeSlice,
  TimeAdd,
  TimeSub,
  TimeDiff,
  TimeTrunc,
  DateFromParts,
  TimeFromParts,
  DateStrToDate,
  DateToDateStr,
  DateToDi,
  Date,
  Day,
  Decode,
  DecodeCase,
  Decrypt,
  DecryptRaw,
  DenseRank,
  DiToDate,
  Encode,
  Encrypt,
  EncryptRaw,
  EqualNull,
  Exp,
  Factorial,
  Explode,
  Inline,
  ExplodeOuter,
  Posexplode,
  PosexplodeOuter,
  PositionalColumn,
  Unnest,
  Floor,
  FromBase32,
  FromBase64,
  ToBase32,
  ToBase64,
  ToBinary,
  Base64DecodeBinary,
  Base64DecodeString,
  Base64Encode,
  TryBase64DecodeBinary,
  TryBase64DecodeString,
  TryHexDecodeBinary,
  TryHexDecodeString,
  FromISO8601Timestamp,
  GapFill,
  GenerateDateArray,
  GenerateTimestampArray,
  GetExtract,
  Getbit,
  Greatest,
  OverflowTruncateBehavior,
  GroupConcat,
  Hex,
  HexDecodeString,
  HexEncode,
  Hour,
  Minute,
  Second,
  Compress,
  DecompressBinary,
  DecompressString,
  LowerHex,
  And,
  Or,
  Xor,
  If,
  Nullif,
  Initcap,
  IsAscii,
  IsNan,
  Int64,
  IsInf,
  IsNullValue,
  IsArray,
  JSON,
  JSONPath,
  JSONPathPart,
  JSONPathFilter,
  JSONPathKey,
  JSONPathRecursive,
  JSONPathRoot,
  JSONPathScript,
  JSONPathSlice,
  JSONPathSelector,
  JSONPathSubscript,
  JSONPathUnion,
  JSONPathWildcard,
  FormatJson,
  Format,
  JSONKeys,
  JSONKeyValue,
  JSONKeysAtDepth,
  JSONObject,
  JSONObjectAgg,
  JSONBObjectAgg,
  JSONArray,
  JSONArrayAgg,
  JSONExists,
  JSONColumnDef,
  JSONSchema,
  JSONSet,
  JSONStripNulls,
  JSONValue,
  JSONValueArray,
  JSONRemove,
  JSONTable,
  JSONType,
  ObjectInsert,
  OpenJSONColumnDef,
  OpenJSON,
  JSONBContains,
  JSONBContainsAnyTopKeys,
  JSONBContainsAllTopKeys,
  JSONBExists,
  JSONBDeleteAtPath,
  JSONExtract,
  JSONExtractQuote,
  JSONExtractArray,
  JSONExtractScalar,
  JSONBExtract,
  JSONBExtractScalar,
  JSONFormat,
  JSONArrayAppend,
  JSONArrayContains,
  JSONArrayInsert,
  ParseBignumeric,
  ParseNumeric,
  ParseJSON,
  ParseUrl,
  ParseIp,
  ParseTime,
  ParseDatetime,
  Least,
  Left,
  Right,
  Reverse,
  Length,
  RtrimmedLength,
  BitLength,
  Levenshtein,
  Ln,
  Log,
  LogicalOr,
  LogicalAnd,
  Lower,
  Map,
  ToMap,
  MapFromEntries,
  MapCat,
  MapContainsKey,
  MapDelete,
  MapInsert,
  MapKeys,
  MapPick,
  MapSize,
  ScopeResolution,
  Slice,
  Stream,
  StarMap,
  VarMap,
  MatchAgainst,
  Max,
  MD5,
  MD5Digest,
  MD5NumberLower64,
  MD5NumberUpper64,
  Median,
  Mode,
  Min,
  Month,
  Monthname,
  AddMonths,
  Nvl2,
  Ntile,
  Normalize,
  Normal,
  NetFunc,
  Host,
  RegDomain,
  Overlay,
  Predict,
  MLTranslate,
  FeaturesAtTime,
  GenerateEmbedding,
  MLForecast,
  ModelAttribute,
  VectorSearch,
  Pi,
  Pow,
  PercentileCont,
  PercentileDisc,
  PercentRank,
  Quantile,
  ApproxQuantile,
  ApproxPercentileAccumulate,
  ApproxPercentileEstimate,
  Quarter,
  Rand,
  Randn,
  Randstr,
  RangeN,
  RangeBucket,
  Rank,
  ReadCSV,
  ReadParquet,
  Reduce,
  RegexpExtract,
  RegexpExtractAll,
  RegexpReplace,
  RegexpLike,
  RegexpILike,
  RegexpFullMatch,
  RegexpInstr,
  RegexpSplit,
  RegexpCount,
  RegrValx,
  RegrValy,
  RegrAvgy,
  RegrAvgx,
  RegrCount,
  RegrIntercept,
  RegrR2,
  RegrSxx,
  RegrSxy,
  RegrSyy,
  RegrSlope,
  Repeat,
  Replace,
  Radians,
  Round,
  RowNumber,
  Seq1,
  Seq2,
  Seq4,
  Seq8,
  SafeAdd,
  SafeDivide,
  SafeMultiply,
  SafeNegate,
  SafeSubtract,
  SafeConvertBytesToString,
  SHA,
  SHA2,
  SHA1Digest,
  SHA2Digest,
  Sign,
  SortArray,
  Soundex,
  SoundexP123,
  Split,
  SplitPart,
  Substring,
  SubstringIndex,
  StandardHash,
  StartsWith,
  EndsWith,
  StrPosition,
  Search,
  SearchIp,
  StrToDate,
  StrToTime,
  StrToUnix,
  StrToMap,
  NumberToStr,
  FromBase,
  Space,
  Struct,
  StructExtract,
  Stuff,
  Sum,
  Sqrt,
  Stddev,
  StddevPop,
  StddevSamp,
  Time,
  TimeToStr,
  TimeToTimeStr,
  TimeToUnix,
  TimeStrToDate,
  TimeStrToTime,
  TimeStrToUnix,
  Trim,
  TsOrDsAdd,
  TsOrDsDiff,
  TsOrDsToDateStr,
  TsOrDsToDate,
  TsOrDsToDatetime,
  TsOrDsToTime,
  TsOrDsToTimestamp,
  TsOrDiToDi,
  Unhex,
  Unicode,
  Uniform,
  UnixDate,
  UnixToStr,
  UnixToTime,
  UnixToTimeStr,
  UnixSeconds,
  UnixMicros,
  UnixMillis,
  Uuid,
  TimestampFromParts,
  TimestampLtzFromParts,
  TimestampTzFromParts,
  Upper,
  Corr,
  CumeDist,
  Variance,
  VariancePop,
  Kurtosis,
  Skewness,
  WidthBucket,
  CovarSamp,
  CovarPop,
  Week,
  WeekStart,
  NextDay,
  XMLElement,
  XMLGet,
  XMLTable,
  XMLNamespace,
  XMLKeyValueOption,
  Year,
  Zipf,
  Use,
  Merge,
  When,
  Whens,
  NextValueFor,
  Semicolon,
  TableColumn,
  Variadic,
];

export const MULTI_INHERITANCE_MAP: Record<string, readonly string[]> = {
  'AggFunc': ['Corr'],
  'DML': ['Insert'],
  'DerivedTable': ['Explode', 'ExplodeOuter', 'Generator', 'Posexplode', 'PosexplodeOuter', 'Unnest'],
  'ExplodeOuter': ['PosexplodeOuter'],
  'Func': ['And', 'ArrayContains', 'ArrayContainsAll', 'ArrayOverlaps', 'Collate', 'Corr', 'JSONArrayContains', 'JSONBContains', 'JSONBContainsAllTopKeys', 'JSONBContainsAnyTopKeys', 'JSONBDeleteAtPath', 'JSONBExtract', 'JSONBExtractScalar', 'JSONExtract', 'JSONExtractScalar', 'Or', 'Pow', 'RegexpFullMatch', 'RegexpILike', 'RegexpLike', 'Xor'],
  'IntervalOp': ['DateAdd', 'DateBin', 'DateSub', 'DatetimeAdd', 'DatetimeSub'],
  'Predicate': ['EQ', 'Exists', 'GT', 'GTE', 'Glob', 'ILike', 'Is', 'JSONArrayContains', 'LT', 'LTE', 'Like', 'Match', 'NEQ', 'NullSafeEQ', 'NullSafeNEQ', 'SimilarTo'],
  'Property': ['Tags'],
  'Query': ['Subquery'],
  'SubqueryPredicate': ['Exists'],
  'TimeUnit': ['DateAdd', 'DateBin', 'DateDiff', 'DateSub', 'DatetimeAdd', 'DatetimeDiff', 'DatetimeSub', 'DatetimeTrunc', 'LastDay', 'TimeAdd', 'TimeDiff', 'TimeSlice', 'TimeSub', 'TimeTrunc', 'TimestampAdd', 'TimestampDiff', 'TimestampSub', 'TimestampTrunc', 'TsOrDsAdd', 'TsOrDsDiff'],
  'UDTF': ['Explode', 'ExplodeOuter', 'Generator', 'Posexplode', 'PosexplodeOuter', 'Unnest'],
};
