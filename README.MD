## dbt_abc_package

This dbt package contains non-standard materializations, macros for dummy data and models for performance analysis.

## Installation

To use this package, include it in your `packages.yml` file:

```yml
packages:
  - package: dbt_abc_package
    git: https://github.com/your_username/dbt_abc_package.git
    revision: 0.4.0
```

Then, run dbt deps to install the package.

## Materializations

This package includes the following materializations:

**abc_package_table_by_parts**

This is a dbt materialization written in Jinja, a template engine used in Python. It defines a new table called `abc_package_table_by_parts` using an input model specified in the config section, and splits the input data into count_of_parts partitions based on the quantile values stored in the quantile_table and quantile_column provided in the config section. It then runs SQL queries to insert the data into the new table in each partition.

The `abc_package_table_by_parts` materialization macro appears to be using a macro called `mcr_get_table_ranges_by_quantile` to split the input data into multiple parts based on the specified quantiles.

How to use:

```jinja
{{ config(materialized    = 'abc_package_table_by_parts',
          input_model     = 'int_attribution_goals_preparation',
          quantile_table  = 'attribution__union',
          quantile_column = 'global_account_id',
          count_of_parts  = 10 ) }}
```

* `input_model`: the name of the model or relation to be materialized. Filters will be add to this source
* `quantile_table`: the name of the table containing quantiles used to split the input data. Quantiles ranges are calculated for this table
* `quantile_column`: the name of the column used to calculate the quantiles
* `count_of_parts`: the number of parts to split the input data into. Optional, default value = 10

input_model example:
```sql
WITH
    attr_union AS     (
        SELECT * FROM {{ ref('int_attribution_goals_preparation') }}
),
```

## Models

This package includes the following models:

**abc_package_query_log**: This model can be used to analyze BigQuery job performance and troubleshoot any errors that may occur during queries execution.
[Details](docs/abc_package_query_log.md)

## Macros

This package includes the following macros:

TODO

## Configuration

TODO: Example

This package requires the following variables to be set in your dbt_project.yml file:

```yaml
vars:
  abc_database: your_abc_database
  abc_schema: your_abc_schema
```

## Usage

To use this package, include the models and macros in your dbt project. For example:

```yaml
models:
  - name: abc_package_query_log
    description: "{{ docs('abc_package_query_log.md') }}"
    columns:
      - name: gbq_job_project_id
        description: The ID of the project where the job was run.
      ...
    
macros:
  - name: abc_date_trunc
    description: Truncates a timestamp to the nearest hour, day, week, or month.
    ...
```

You can then run dbt run to build the models and macros, and dbt test to test the results.

## Contributing

If you find any issues or have suggestions for improvements, please feel free to submit a pull request or open an issue.

## License

This package is released under the MIT License. See `LICENSE` for more information.
