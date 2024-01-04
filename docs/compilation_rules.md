# Compilation rules
## Relation

A `Relation` is an intermediate representation that allow to easily handle DP transformations which can be resumed in 2 quetions:
- what is the protected unit ?
- what is the sensitivity ?
In fact each `Relation` variant has a specific behaviour regarding these two questions.


Any SQL query can be transformed into a tree of `Relation`s.

## Properties

- `Public`
- `Published`
- `Private`
- `PrivacyUnitPreserving`
- `DifferentiallyPrivate`
- `SyntheticData`

### Table
A `Relation::Table` may be:
- `Private`
- `PrivacyUnitPreserving`
- `Public`
- `SyntheticData`

### Map
| input | map |
|:----------|:----------|
| `Public` | `Public` |
| `Published` | `Published` |
| `DifferentiallyPrivate` | `Published` |
| `PrivacyUnitPreserving` | `PrivacyUnitPreserving` |
| `SyntheticData` | `SyntheticData` |

### Reduce
| input | condition | reduce |
|:----------|:----------| :----------|
| `Public` | | `Public` |
| `Published` | | `Published` |
| `SyntheticData` | | `SyntheticData` |
| `PrivacyUnitPreserving` | all the aggregations are DP-compilable |`DifferentiallyPrivate` |

### Join
| right | left | join |
|:----------|:----------|:----------|
| `Public` | `Public` | `Public` |
| `Published` | `Published` | `Published` |
| `Published` | `PrivacyUnitPreserving` | `PrivacyUnitPreserving` |
| `PrivacyUnitPreserving` | `Published` | `PrivacyUnitPreserving` |
| `PrivacyUnitPreserving` | `PrivacyUnitPreserving` | `PrivacyUnitPreserving` |
| `DifferentiallyPrivate` | `PrivacyUnitPreserving` | `PrivacyUnitPreserving` |
| `PrivacyUnitPreserving` | `DifferentiallyPrivate` | `PrivacyUnitPreserving` |
| `SyntheticData` | `SyntheticData` | `SyntheticData` |

### Set
| right | left | join |
|:----------|:----------|:----------|
| `Public` | `Public` | `Public` |
| `Published` | `Published` | `Published` |
| `Published` | `PrivacyUnitPreserving` | `PrivacyUnitPreserving` |
| `SyntheticData` | `SyntheticData` | `SyntheticData` |

### Values
A `Relation::Values` may be:
- `Public`
- `SyntheticData`


## Rewriting
The rewriting process consists of three sequential steps:

1. **Initial Assignment**: Each `Relation` is assigned a set of rules based solely on its type. For instance, if it contains only supported aggregations, a `Reduce` may be categorized as `Public`, `Published`, `SyntheticData`, or `DifferentiallyPrivate`.

2. **Bottom-Up Rule Selection**: Starting from the bottom of the tree, rules are selected based on the possible rules of the input(s). For example, if the input of a `Reduce` is `Public`, then the `Reduce` can only be categorized as `Public`. The other properties are eliminated during this step.

3. **Scoring**: Each property is assigned a score:
    - `Public`: 10
    - `Published`: 1
    - `Private`: 0
    - `PrivacyUnitPreserving`: 2
    - `DifferentiallyPrivate`: 5
    - `SyntheticData`: 1
The higher the score, the more desirable the property. The score of a graph of relations with properties is the sum of the scores of all the relations. The combination of rules that maximizes the total score is selected.

