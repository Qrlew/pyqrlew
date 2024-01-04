# Postgres supported functions and operators
Functions or operators not listed in the following tables are not supported.

## Logical Operators
| Operators | Supported |
|:----------|:----------:|
| `AND` or `&`  | ✓  |
| `OR` or `\|`  | ✓  |
| `NOT`   | ✓  |
| `~`   | ✘  |

## Comparison Operators
| Operators | Supported |
|:----------|:----------:|
| `<`   | ✓  |
| `>`   | ✓  |
| `<=`   | ✓  |
| `>=`   | ✓  |
| `=`   | ✓  |
| `<>` or `!=`   | ✓  |

## Mathematical Functions and Operators
| Operators | Supported |
|:----------|:----------:|
| `+`   | ✓  |
| `-`   | ✓  |
| `*`   | ✓  |
| `/`   | ✓  |
| `%`   | ✓  |
| `^`   | ✓  |
| `\|/`   | ✘  |
| `\|\|/`   | ✘  |
| `@`   | ✘  |
| `!`   | ✘  |

| Functions | Supported |
|:----------|:----------:|
| `abs `   | ✓  |
| `cbrt`   | ✘  |
| `ceil` or `ceiling`    | ✓  |
| `degrees`   |   |
| `div`   | ✘  |
| `exp`   | ✓  |
| `floor`   | ✓  |
| `ln`   | ✓  |
| `log`   | ✓  |
| `mod`   | ✘  |
| `pi`   | ✓  |
| `power`   | ✓  |
| `radians`   | ✘  |
| `round`   | ✓  |
| `sign`   | ✓  |
| `sqrt`   | ✓  |
| `trunc`   | ✓  |
| `random`   | ✓  |
| `acos`   | ✘  |
| `asin`   | ✘  |
| `atan`   | ✘  |
| `atan2`   | ✘  |
| `cos`   | ✓  |
| `cot`   | ✘  |
| `sin`   | ✓  |
| `tan`   | ✓  |


## String Functions and Operators
| Functions | Supported |
|:----------|:----------:|
| `\|\|`   | ✓  |
| `lower(string)`   | ✓  |
| `position(substring in string)`   | ✓  |
| `substring(string [from int \| pattern] [for int \| pattern])`  | ✓  |
| `trim([leading \| trailing \| both] [characters] from string)`  | ✓  |
| `upper(string)`   | ✓  |
| `lower(string)`   | ✓  |
| `ascii(string)`   | ✘  |
| `btrim(string text, characters text)`   | ✓  |
| `btrim(string)`   | ✘  |
| `decode(string text, format text)`   | ✘  |
| `encode(data bytea, format text)`   | ✓ |
| `chr(string)`   | ✓  |
| `ltrim(string text, characters text)`   | ✓  |
| `ltrim(string)`   | ✘  |
| `md5(string)`   | ✓  |
| `rtrim(string text, characters text)`   | ✓  |
| `rtrim(string)`   | ✘  |
| `substr(string, from)`   | ✓  |
| `substr(string, from, count)`   | ✘   |

## Patern Matching

| Expressions | Compilable into DP |
|:----------|:----------:|
| `string [NOT] LIKE pattern`   | planned |
| `string [NOT] LIKE pattern ESCAPE escape-character`   | ✘ |
| `string [NOT] SIMILAR pattern [ESCAPE escape-character]`   | ✘ |

## Date/Time Functions and Operators
| Operators | Supported |
|:----------|:----------:|
| `+`   | ✓  |
| `-`   | ✓  |
| `*`   | ✓  |
| `/`   | ✓  |

| Functions | Compilable into DP |
|:----------|:----------:|
| `current_date`   | ✓  |
| `current_time`   | ✓  |
| `current_timestamp`   | ✓  |
| `date_part(text, timestamp)`   | ✘  |
| `date_trunc(text, timestamp)`   | ✘  |
| `extract(field from timestamp)`   | planned  |
| `now()`   | ✘  |


## Conditional Expressions
| Expressions | Compilable into DP |
|:----------|:----------:|
| `CASE WHEN condition THEN result [WHEN ...] [ELSE result] END`| ✓ |
| `COALESCE(value [, ...])`   | ✓ |
| `NULLIF(value1, value2)`   | ✘ |
| `GREATEST(value [, ...])`   | ✘ |
| `LEAST(value [, ...])`   | ✘ |

## Aggregation functions
| Aggregations | Compilable into DP |
|:----------|:----------:|
| `COUNT (*)`   | ✓  |
| `SUM ([DISTINCT] column)`   | ✓  |
| `AVG ([DISTINCT] column)`   | ✓  |
| `VARIANCE ([DISTINCT] column)`   | ✓  |
| `STDDEV ([DISTINCT] column)`   | ✓  |
| `MIN ([DISTINCT] column)`   |  ✘ |
| `MAX ([DISTINCT] column)`   |  ✘ |

## Row and Array Comparisons
| Expressions | Compilable into DP |
|:----------|:----------:|
| `expression IN (value [, ...])`| ✓ |
| `expression NOT IN (value [, ...])`   | ✓ |

