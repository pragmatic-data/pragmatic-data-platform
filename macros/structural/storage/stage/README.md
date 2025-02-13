# Stage Macros

### Building staging models

#### Stage ([source](stage.sql)) ([docs](stage_macros_docs.yml))

The macro to build a STAGE model (default prefix STG) from metadata passed as YAML.

**Usage with local YAML definition:**

The simplest usage is by having the metadata definitions in YAML in the model itself.
```text
{%- set local_yaml_config -%}
  ... <YAML definition of the parameters>
{%- endset -%}

{%- set metadata_dict = fromyaml(local_yaml_config) -%}
```
The YAML is parsed, converted into a Python dictionary and its top levels passed to the macro.
```text
{{- pragmatic_data.stage(
    source_model            = ref('GENERIC_TWO_COLUMN_TABLE'),
    source                  = metadata_dict['source'],
    calculated_columns      = metadata_dict['calculated_columns'],
    hashed_columns          = metadata_dict['hashed_columns'],
    default_records         = metadata_dict['default_records'],
    remove_duplicates       = metadata_dict['remove_duplicates'],
) }}
```

**Usage with external YAML definition:**

An alternative usage of the macro is with YAML stored in a separate YAML file.  
In such a case common configurations can be defined once in YAML and recalled by the use of YAML anchors (& and *).  
The YAML config must be under the `config`attribute of the model containing the stage macro.
```yaml
  - name: YOUR_STAGE_MODEL
    config:
      source_columns:     *stg_model_source_columns
      source:             *stg_model_source
      calculated_columns: *stg_model_calculated_columns
      default_records:    *security_default_records
      hashed_columns:     *stg_model_hashed_columns
      remove_duplicates: 

```
The above config is used in the model named `YOUR_STAGE_MODEL` as shown below:
```text
{{ pragmatic_data.stage(
    source_model            = ref('GENERIC_TWO_COLUMN_TABLE'),
    source                  = config.require('source'),
    calculated_columns      = config.require('calculated_columns'),
    hashed_columns          = config.require('hashed_columns'),
    default_records         = config.require('default_records'),
    remove_duplicates       = config.require('remove_duplicates')
) }}
```