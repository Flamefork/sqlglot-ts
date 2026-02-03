import { Expression, type ExpressionConstructor } from "./expression-base.js"
import { GENERATED_CLASSES } from "./expressions.generated.js"

const INDEX = "i"
const ARG_KEY = "k"
const IS_ARRAY = "a"
const CLASS = "c"
const COMMENTS = "o"
const META = "m"
const VALUE = "v"

type Payload = Record<string, unknown>

type NamedExpressionConstructor = ExpressionConstructor & {
  readonly className: string
}

const CLASS_MAP: Map<string, ExpressionConstructor> = new Map()
for (const cls of GENERATED_CLASSES as readonly NamedExpressionConstructor[]) {
  CLASS_MAP.set(cls.className, cls)
}

export function dump(expression: Expression): Payload[] {
  let i = 0
  const payloads: Payload[] = []
  const stack: Array<
    [unknown, number | undefined, string | undefined, boolean]
  > = [[expression, undefined, undefined, false]]

  while (stack.length > 0) {
    const [node, index, argKey, isArray] = stack.pop()!

    const payload: Payload = {}

    if (index !== undefined) {
      payload[INDEX] = index
    }
    if (argKey !== undefined) {
      payload[ARG_KEY] = argKey
    }
    if (isArray) {
      payload[IS_ARRAY] = isArray
    }

    payloads.push(payload)

    if (node instanceof Expression) {
      payload[CLASS] = (
        node.constructor as NamedExpressionConstructor
      ).className

      if (node.comments) {
        payload[COMMENTS] = node.comments
      }
      if (node._meta !== undefined) {
        payload[META] = node._meta
      }

      const args = node.args
      const keys = Object.keys(args).reverse()
      for (const k of keys) {
        const vs = args[k]
        if (Array.isArray(vs)) {
          for (let j = vs.length - 1; j >= 0; j--) {
            stack.push([vs[j], i, k, true])
          }
        } else if (vs !== undefined && vs !== null) {
          stack.push([vs, i, k, false])
        }
      }
    } else {
      payload[VALUE] = node
    }

    i++
  }

  return payloads
}

export function load(payloads: Payload[]): Expression
export function load(payloads: undefined | null): undefined
export function load(
  payloads: Payload[] | undefined | null,
): Expression | undefined
export function load(
  payloads: Payload[] | undefined | null,
): Expression | undefined {
  if (!payloads || payloads.length === 0) {
    return undefined
  }

  const [first, ...tail] = payloads as [Payload, ...Payload[]]
  const root = _load(first)
  const nodes: unknown[] = [root]

  for (const payload of tail) {
    const node = _load(payload)
    nodes.push(node)

    const parent = nodes[payload[INDEX] as number] as Expression
    const argKey = payload[ARG_KEY] as string

    if (payload[IS_ARRAY]) {
      parent.append(argKey, node as Expression)
    } else {
      ;(parent.args as Record<string, unknown>)[argKey] = node
      if (node instanceof Expression) {
        node.parent = parent
        node.argKey = argKey
      }
    }
  }

  return root as Expression
}

function _load(payload: Payload): unknown {
  const className = payload[CLASS] as string | undefined

  if (!className) {
    return payload[VALUE]
  }

  const cls = CLASS_MAP.get(className)
  if (!cls) {
    throw new Error(`Unknown expression class: ${className}`)
  }

  const expression = new cls({})
  expression.comments = payload[COMMENTS] as string[] | undefined
  expression._meta = payload[META] as Record<string, unknown> | undefined
  return expression
}
