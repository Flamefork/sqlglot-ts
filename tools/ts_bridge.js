#!/usr/bin/env node
/**
 * JSON-RPC bridge for TypeScript sqlglot-ts
 * Holds expressions in memory and proxies all operations from Python.
 */

import { createInterface } from "node:readline";
import * as expMod from "../dist/expressions.generated.mjs";
import "../dist/dialects/index.mjs";
import {
	annotateTypes,
	indexOffsetLogs,
	parse,
	parseOne,
	transpile,
} from "../dist/index.mjs";

// Expression store - holds parsed expressions by ID
const expressions = new Map();
let nextId = 1;

function storeExpr(expr) {
	const id = nextId++;
	expressions.set(id, expr);
	return id;
}

function getExpr(id) {
	return expressions.get(id);
}

// Serialize a value for Python
function serialize(val) {
	if (val === null || val === undefined) {
		return { type: "null" };
	}
	if (typeof val === "string") {
		return { type: "string", value: val };
	}
	if (typeof val === "number") {
		return { type: "number", value: val };
	}
	if (typeof val === "boolean") {
		return { type: "boolean", value: val };
	}
	if (Array.isArray(val)) {
		return { type: "array", value: val.map(serialize) };
	}
	if (val && typeof val === "object" && typeof val.key === "string") {
		// It's an Expression
		const id = storeExpr(val);
		return { type: "expr", id, key: val.key };
	}
	if (val && typeof val === "object") {
		// Plain object (like args)
		const result = {};
		for (const [k, v] of Object.entries(val)) {
			result[k] = serialize(v);
		}
		return { type: "object", value: result };
	}
	return { type: "unknown", value: String(val) };
}

// Deserialize arguments from Python
function deserializeArg(arg) {
	if (arg && typeof arg === "object" && "__expr_id__" in arg) {
		return getExpr(arg.__expr_id__);
	}
	if (Array.isArray(arg)) {
		return arg.map(deserializeArg);
	}
	if (arg && typeof arg === "object") {
		const result = {};
		for (const [k, v] of Object.entries(arg)) {
			result[k] = deserializeArg(v);
		}
		return result;
	}
	return arg;
}

// Custom error class for unsupported operations
class UnsupportedError extends Error {
	constructor(message) {
		super(message);
		this.name = "UnsupportedError";
	}
}

function drainLogs() {
	const logs = indexOffsetLogs.splice(0);
	return logs.length > 0 ? logs : undefined;
}

const rl = createInterface({
	input: process.stdin,
	output: process.stdout,
	terminal: false,
});

rl.on("line", (line) => {
	try {
		const cmd = JSON.parse(line);
		let result;

		switch (cmd.method) {
			case "parse": {
				const exprs = parse(cmd.sql, { dialect: cmd.dialect || "" });
				const ids = exprs.map((e) => storeExpr(e));
				result = { ok: true, ids, keys: exprs.map((e) => e.key) };
				break;
			}

			case "parseOne": {
				drainLogs();
				const expr = parseOne(cmd.sql, { dialect: cmd.dialect || "" });
				const id = storeExpr(expr);
				const parseLogs = drainLogs();
				result = { ok: true, id, key: expr.key };
				if (parseLogs) result.logs = parseLogs;
				break;
			}

			case "transpile": {
				const results = transpile(cmd.sql, {
					read: cmd.readDialect || "",
					write: cmd.writeDialect || "",
				});
				result = { ok: true, sql: results };
				break;
			}

			case "sql": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					const opts = {};
					if (cmd.dialect) opts.dialect = cmd.dialect;
					if (cmd.pretty) opts.pretty = cmd.pretty;
					if (cmd.identify) opts.identify = cmd.identify;
					if (cmd.unsupportedLevel)
						opts.unsupportedLevel = cmd.unsupportedLevel;

					try {
						drainLogs();
						const sql = expr.sql(opts);
						const sqlLogs = drainLogs();
						result = { ok: true, sql };
						if (sqlLogs) result.logs = sqlLogs;
					} catch (err) {
						if (
							err instanceof UnsupportedError ||
							err.name === "UnsupportedError"
						) {
							result = {
								ok: false,
								error: String(err.message || err),
								errorType: "UnsupportedError",
							};
						} else {
							result = {
								ok: false,
								error: String(err.message || err),
								errorType: err.name || "Error",
							};
						}
					}
				}
				break;
			}

			case "getattr": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					const val = expr[cmd.name];
					if (typeof val === "function") {
						result = { ok: true, value: { type: "method", name: cmd.name } };
					} else {
						result = { ok: true, value: serialize(val) };
					}
				}
				break;
			}

			case "call": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					const method = expr[cmd.name];
					if (typeof method !== "function") {
						result = { ok: false, error: `${cmd.name} is not a method` };
					} else {
						const args = (cmd.args || []).map(deserializeArg);
						const kwargs = cmd.kwargs ? deserializeArg(cmd.kwargs) : {};
						if (Object.keys(kwargs).length > 0) {
							args.push(kwargs);
						}
						const ret = method.apply(expr, args);
						result = { ok: true, value: serialize(ret) };
					}
				}
				break;
			}

			case "text": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					const text = expr.text(cmd.name);
					result = { ok: true, value: text };
				}
				break;
			}

			case "release": {
				// Release expressions to free memory
				for (const id of cmd.ids || []) {
					expressions.delete(id);
				}
				result = { ok: true };
				break;
			}

			case "assertIs": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					const ExpectedClass = expMod[cmd.expectedKey];
					const extraKeys = expMod.MULTI_INHERITANCE_MAP[cmd.expectedKey] || [];
					if (ExpectedClass && expr instanceof ExpectedClass) {
						result = { ok: true };
					} else if (expr.key.toLowerCase() === cmd.expectedKey.toLowerCase()) {
						result = { ok: true };
					} else if (
						extraKeys.some((k) => expr.key.toLowerCase() === k.toLowerCase())
					) {
						result = { ok: true };
					} else {
						result = {
							ok: false,
							error: `Expected ${cmd.expectedKey}, got ${expr.key}`,
						};
					}
				}
				break;
			}

			case "find": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					const targetKey = cmd.exprType.toLowerCase();
					const TargetClass = expMod[cmd.exprType];
					const extraKeys = expMod.MULTI_INHERITANCE_MAP[cmd.exprType] || [];
					let found = null;
					for (const node of expr.bfs()) {
						if (
							(TargetClass && node instanceof TargetClass) ||
							node.key === targetKey ||
							extraKeys.some((k) => node.key.toLowerCase() === k.toLowerCase())
						) {
							found = node;
							break;
						}
					}
					if (found) {
						const foundId = storeExpr(found);
						result = {
							ok: true,
							value: { type: "expr", id: foundId, key: found.key },
						};
					} else {
						result = { ok: true, value: { type: "null" } };
					}
				}
				break;
			}

			case "findAll": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					const targetKey = cmd.exprType.toLowerCase();
					const TargetClass = expMod[cmd.exprType];
					const extraKeys = expMod.MULTI_INHERITANCE_MAP[cmd.exprType] || [];
					const found = [];
					for (const node of expr.bfs()) {
						if (
							(TargetClass && node instanceof TargetClass) ||
							node.key === targetKey ||
							extraKeys.some((k) => node.key.toLowerCase() === k.toLowerCase())
						) {
							const fid = storeExpr(node);
							found.push({ type: "expr", id: fid, key: node.key });
						}
					}
					result = { ok: true, values: found };
				}
				break;
			}

			case "annotateTypes": {
				const expr = getExpr(cmd.id);
				if (!expr) {
					result = { ok: false, error: `Expression ${cmd.id} not found` };
				} else {
					annotateTypes(expr);
					result = { ok: true, id: cmd.id, key: expr.key };
				}
				break;
			}

			case "createExpression": {
				// Create a new expression by class name with given args
				const ExprClass = expMod[cmd.className];
				if (!ExprClass) {
					result = {
						ok: false,
						error: `Unknown expression class: ${cmd.className}`,
					};
				} else {
					const args = {};
					for (const [k, v] of Object.entries(cmd.args || {})) {
						args[k] = deserializeArg(v);
					}
					const expr = new ExprClass(args);
					const id = storeExpr(expr);
					result = { ok: true, id, key: expr.key };
				}
				break;
			}

			default:
				result = { ok: false, error: `Unknown method: ${cmd.method}` };
		}

		console.log(JSON.stringify(result));
	} catch (err) {
		console.log(JSON.stringify({ ok: false, error: String(err) }));
	}
});

rl.on("close", () => {
	process.exit(0);
});
