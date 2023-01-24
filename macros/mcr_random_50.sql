--scheme: https://www.figma.com/file/xrmg3yb2Czynz5MNQzwTou/Dummy-Data-From-raw-to-MCDM?node-id=104%3A390&t=0DkhJaz9cQ6cUyLe-4

{%- macro mcr_random_50() -%}
        
CAST(round(rand() / 4294967295 * 50 + 0.5) AS String)

{%- endmacro -%}