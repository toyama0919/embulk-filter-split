# Split filter plugin for Embulk

split record with delimiter.

## Overview

* **Plugin type**: filter

## Configuration

- **delimiter**: delimiter. (boolean, default: `,`)
- **keep_input**: keep input columns. (boolean, default: `true`)
- **target_key**: target key name. (string, required)
- **output_key**: output key name. (string, default: `null`)

## Example(split column output other column)

```yaml
  - type: split
    delimiter: ','
    keep_input: true
    target_key: keywords
    output_key: keyword
```

### input

```
{ "keywords" : "Ruby,Java,Python" }
```

As below

```
{ "keywords" : "Ruby,Java,Python", "keyword" : "Ruby" }
{ "keywords" : "Ruby,Java,Python", "keyword" : "Java" }
{ "keywords" : "Ruby,Java,Python", "keyword" : "Python" }
```

## Example(split column overwrite)

```yaml
  - type: split
    delimiter: ','
    keep_input: true
    target_key: keywords
```

### input

```
{ "keywords" : "Ruby,Java,Python", "keep_column" : "test" }
```

As below

```
{ "keywords" : "Ruby", "keep_column" : "test" }
{ "keywords" : "Java", "keep_column" : "test" }
{ "keywords" : "Python", "keep_column" : "test" }
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
