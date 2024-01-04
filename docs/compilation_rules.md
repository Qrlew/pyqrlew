# Compilation rules
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