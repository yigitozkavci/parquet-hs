# Overview for Decoding Logic

Required reading:
1. High level overview of Parquet format: Required reading: https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
2. Overall format of a Parquet file: https://parquet.apache.org/documentation/latest/#file-format

Entry point for decoding in the codebase is in Parquet.Reader.readWholeParquetFile. This function does the following in order:
1. For each row group, reads the column chunks (via Parquet.Reader.sourceColumnChunk) & associates them with their paths. Some example paths
  - "f1.list.element.list.element" would represent any value in the column { "f1": [[1, 2], [3, 4], [5]] }. By using the repetition & definition levels, we are able to represent the following possible values that belong to "f1.list.element.list.element" path:
    - `{}`
    - `{ "f1": null }`
    - `{ "f1": [] }`
    - `{ "f1": [[1]] }`
    - `{ "f1": [[1], [3]] }`
    - `{ "f1": [[1, 2], [3, 4], [5]] }`,
  - "f3.list.element" would represent any value in the column { "f3": [1, 2, 3] }. Notice that we only allow a single dimension list here. This is because in the file schema, "f3" has OPTIONAL type, "f3.list" has REPEATED and "f3.list.element" has OPTIONAL type. This schema allows for only a single level list. Following values are representable on this path with various repetition & definition levels:
    - `{}`
    - `{ "f3": null }`
    - `{ "f3": [] }`
    - `{ "f3": [1] }`
    - `{ "f3": [3] }`
    - `{ "f3": [1, 2, 3] }`

2. After having all the column chunks for a given path, we can start constructing out record incrementally. Parquet.Reader.generateInstructions is responsible for generating instructions from a set of ColumnValue`s for a given path. Some example instructions that are generated:
  - Example 1
    - Path: `f1.list.element.list.element`
    - Set of column values to construct the column:
      ```
			[ ColumnValue
					{ _cvRepetitionLevel = 0
					, _cvDefinitionLevel = 5
					, _cvMaxDefinitionLevel = 5
					, _cvValue = ValueInt64 1
					}
        -- Generated instruction set: Just (fromList [IObjectField "f1",INewList,IObjectField "element",INewList,IObjectField "element",IValue (ValueInt64 1)])
			, ColumnValue
					{ _cvRepetitionLevel = 2
					, _cvDefinitionLevel = 5
					, _cvMaxDefinitionLevel = 5
					, _cvValue = ValueInt64 2
					}
        -- Generated instruction set: Just (fromList [IObjectField "f1",IListElement,IObjectField "element",INewListElement,IObjectField "element",IValue (ValueInt64 2)])
			, ColumnValue
					{ _cvRepetitionLevel = 1
					, _cvDefinitionLevel = 5
					, _cvMaxDefinitionLevel = 5
					, _cvValue = ValueInt64 3
					}
        -- Generated instruction set: Just (fromList [IObjectField "f1",INewListElement,IObjectField "element",INewList,IObjectField "element",IValue (ValueInt64 3)])
			, ColumnValue
					{ _cvRepetitionLevel = 2
					, _cvDefinitionLevel = 5
					, _cvMaxDefinitionLevel = 5
					, _cvValue = ValueInt64 4
					}
        -- Generated instruction set: Just (fromList [IObjectField "f1",IListElement,IObjectField "element",INewListElement,IObjectField "element",IValue (ValueInt64 4)])
			, ColumnValue
					{ _cvRepetitionLevel = 1
					, _cvDefinitionLevel = 5
					, _cvMaxDefinitionLevel = 5
					, _cvValue = ValueInt64 5
					}
        -- Generated instruction set: Just (fromList [IObjectField "f1",INewListElement,IObjectField "element",INewList,IObjectField "element",IValue (ValueInt64 5)])
			, ColumnValue
					{ _cvRepetitionLevel = 0
					, _cvDefinitionLevel = 0
					, _cvMaxDefinitionLevel = 5
					, _cvValue = Null
					}
        -- Generated instruction set: Just (fromList [INullOpt])
			, ColumnValue
					{ _cvRepetitionLevel = 0
					, _cvDefinitionLevel = 0
					, _cvMaxDefinitionLevel = 5
					, _cvValue = Null
					}
        -- Generated instruction set: Just (fromList [INullOpt])
			, ColumnValue
					{ _cvRepetitionLevel = 0
					, _cvDefinitionLevel = 0
					, _cvMaxDefinitionLevel = 5
					, _cvValue = Null
					}
        -- Generated instruction set: Just (fromList [INullOpt])
			, ColumnValue
					{ _cvRepetitionLevel = 0
					, _cvDefinitionLevel = 0
					, _cvMaxDefinitionLevel = 5
					, _cvValue = Null
					}
        -- Generated instruction set: Just (fromList [INullOpt])
			, ColumnValue
					{ _cvRepetitionLevel = 0
					, _cvDefinitionLevel = 0
					, _cvMaxDefinitionLevel = 5
					, _cvValue = Null
					}
        -- Generated instruction set: Just (fromList [INullOpt])
			]
      ```
3. The rest of the work is done by Parquet.Reader.interpretInstructions, which simply reads the list of instructions & generate parquet values out of them.
