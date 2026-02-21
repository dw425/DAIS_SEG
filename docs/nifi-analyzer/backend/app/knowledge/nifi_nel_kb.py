"""
NiFi Expression Language Knowledge Base -- 100+ functions mapped to PySpark.

Every NiFi Expression Language (NEL) function is cataloged with its category,
argument signature, human-readable description, PySpark column-expression
equivalent, and a Python-string-mode equivalent.  Placeholder tokens use
curly-brace interpolation so callers can substitute concrete column references
or Python variable names at code-generation time.

Placeholders
------------
{subject}   -- the value the function is called on (column ref or Python var)
{arg0}      -- first argument
{arg1}      -- second argument
{arg2}      -- third argument

Usage
-----
>>> from app.knowledge.nifi_nel_kb import lookup_nel_function
>>> fn = lookup_nel_function("toUpper")
>>> fn.pyspark_col.format(subject="F.col('name')")
"F.upper(F.col('name'))"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class NELFunctionDef:
    """Definition for a single NiFi Expression Language function."""

    name: str
    category: str  # string, math, date, boolean, encoding, system, multi_value, type_conversion
    args: list[str]  # argument names (empty list for zero-arg functions)
    description: str
    pyspark_col: str  # PySpark column mode, e.g. "F.upper({subject})"
    pyspark_python: str  # Python string mode, e.g. "{subject}.upper()"
    notes: str = ""


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

NEL_FUNCTION_KB: dict[str, NELFunctionDef] = {}


def _register(fn: NELFunctionDef) -> None:
    """Register a function definition in the knowledge base."""
    NEL_FUNCTION_KB[fn.name] = fn


# ===================================================================
# STRING FUNCTIONS  (38 entries)
# ===================================================================

_register(NELFunctionDef(
    name="toUpper",
    category="string",
    args=[],
    description="Converts the subject string to uppercase.",
    pyspark_col="F.upper({subject})",
    pyspark_python="{subject}.upper()",
))

_register(NELFunctionDef(
    name="toLower",
    category="string",
    args=[],
    description="Converts the subject string to lowercase.",
    pyspark_col="F.lower({subject})",
    pyspark_python="{subject}.lower()",
))

_register(NELFunctionDef(
    name="trim",
    category="string",
    args=[],
    description="Removes leading and trailing whitespace from the subject.",
    pyspark_col="F.trim({subject})",
    pyspark_python="{subject}.strip()",
))

_register(NELFunctionDef(
    name="length",
    category="string",
    args=[],
    description="Returns the number of characters in the subject string.",
    pyspark_col="F.length({subject})",
    pyspark_python="len({subject})",
))

_register(NELFunctionDef(
    name="substring",
    category="string",
    args=["startIndex", "endIndex"],
    description="Returns the portion of the subject from startIndex (inclusive) to endIndex (exclusive). NiFi uses 0-based indexing; PySpark substr uses 1-based.",
    pyspark_col="F.substring({subject}, {arg0} + 1, {arg1} - {arg0})",
    pyspark_python="{subject}[{arg0}:{arg1}]",
    notes="NiFi is 0-based; PySpark substr() is 1-based.  The generated code adds 1 to the start index and computes length as (endIndex - startIndex).",
))

_register(NELFunctionDef(
    name="substringBefore",
    category="string",
    args=["value"],
    description="Returns the part of the subject before the first occurrence of *value*.",
    pyspark_col="F.substring_index({subject}, {arg0}, 1)",
    pyspark_python="{subject}.split({arg0}, 1)[0]",
))

_register(NELFunctionDef(
    name="substringAfter",
    category="string",
    args=["value"],
    description="Returns the part of the subject after the first occurrence of *value*.",
    pyspark_col="F.expr(\"substring_index({subject}, {arg0}, -1)\")",
    pyspark_python="{subject}.split({arg0}, 1)[-1]",
    notes="PySpark substring_index with count=-1 returns everything after the last occurrence; use regexp_extract for more precision.",
))

_register(NELFunctionDef(
    name="substringBeforeLast",
    category="string",
    args=["value"],
    description="Returns the part of the subject before the last occurrence of *value*.",
    pyspark_col="F.substring_index({subject}, {arg0}, -1 * (F.size(F.split({subject}, {arg0})) - 1))",
    pyspark_python="{subject}.rsplit({arg0}, 1)[0]",
    notes="PySpark has no direct one-liner; consider substring_index with negative count or UDF.",
))

_register(NELFunctionDef(
    name="substringAfterLast",
    category="string",
    args=["value"],
    description="Returns the part of the subject after the last occurrence of *value*.",
    pyspark_col="F.element_at(F.split({subject}, {arg0}), -1)",
    pyspark_python="{subject}.rsplit({arg0}, 1)[-1]",
))

_register(NELFunctionDef(
    name="replace",
    category="string",
    args=["search", "replacement"],
    description="Replaces all literal occurrences of *search* with *replacement*.",
    pyspark_col="F.regexp_replace({subject}, F.lit({arg0}), F.lit({arg1}))",
    pyspark_python="{subject}.replace({arg0}, {arg1})",
))

_register(NELFunctionDef(
    name="replaceFirst",
    category="string",
    args=["regex", "replacement"],
    description="Replaces the first match of *regex* with *replacement*.",
    pyspark_col="F.regexp_replace({subject}, {arg0}, {arg1})",
    pyspark_python="re.sub({arg0}, {arg1}, {subject}, count=1)",
    notes="PySpark regexp_replace replaces all by default; wrap with a UDF for first-only replacement.",
))

_register(NELFunctionDef(
    name="replaceAll",
    category="string",
    args=["regex", "replacement"],
    description="Replaces all matches of *regex* with *replacement*.",
    pyspark_col="F.regexp_replace({subject}, {arg0}, {arg1})",
    pyspark_python="re.sub({arg0}, {arg1}, {subject})",
))

_register(NELFunctionDef(
    name="replaceLast",
    category="string",
    args=["regex", "replacement"],
    description="Replaces the last match of *regex* with *replacement*.",
    pyspark_col="F.regexp_replace({subject}, '(.*)' + {arg0}, '$1' + {arg1})",
    pyspark_python="re.sub(r'(.*)' + {arg0}, r'\\\\1' + {arg1}, {subject})",
    notes="Uses a greedy-prefix capture group to target the last match.",
))

_register(NELFunctionDef(
    name="replaceNull",
    category="string",
    args=["replacement"],
    description="Replaces the subject with *replacement* if the subject is null.",
    pyspark_col="F.coalesce({subject}, F.lit({arg0}))",
    pyspark_python="{subject} if {subject} is not None else {arg0}",
))

_register(NELFunctionDef(
    name="replaceEmpty",
    category="string",
    args=["replacement"],
    description="Replaces the subject with *replacement* if the subject is null or empty.",
    pyspark_col="F.when(F.coalesce(F.length(F.trim({subject})), F.lit(0)) == 0, F.lit({arg0})).otherwise({subject})",
    pyspark_python="{subject} if {subject} else {arg0}",
))

_register(NELFunctionDef(
    name="append",
    category="string",
    args=["value"],
    description="Appends *value* to the end of the subject string.",
    pyspark_col="F.concat({subject}, F.lit({arg0}))",
    pyspark_python="{subject} + {arg0}",
))

_register(NELFunctionDef(
    name="prepend",
    category="string",
    args=["value"],
    description="Prepends *value* to the beginning of the subject string.",
    pyspark_col="F.concat(F.lit({arg0}), {subject})",
    pyspark_python="{arg0} + {subject}",
))

_register(NELFunctionDef(
    name="contains",
    category="string",
    args=["value"],
    description="Returns true if the subject contains *value*.",
    pyspark_col="{subject}.contains({arg0})",
    pyspark_python="{arg0} in {subject}",
))

_register(NELFunctionDef(
    name="startsWith",
    category="string",
    args=["value"],
    description="Returns true if the subject starts with *value*.",
    pyspark_col="{subject}.startswith({arg0})",
    pyspark_python="{subject}.startswith({arg0})",
))

_register(NELFunctionDef(
    name="endsWith",
    category="string",
    args=["value"],
    description="Returns true if the subject ends with *value*.",
    pyspark_col="{subject}.endswith({arg0})",
    pyspark_python="{subject}.endswith({arg0})",
))

_register(NELFunctionDef(
    name="equals",
    category="string",
    args=["value"],
    description="Returns true if the subject equals *value* (case-sensitive).",
    pyspark_col="{subject} == F.lit({arg0})",
    pyspark_python="{subject} == {arg0}",
))

_register(NELFunctionDef(
    name="equalsIgnoreCase",
    category="string",
    args=["value"],
    description="Returns true if the subject equals *value* ignoring case.",
    pyspark_col="F.lower({subject}) == F.lower(F.lit({arg0}))",
    pyspark_python="{subject}.lower() == {arg0}.lower()",
))

_register(NELFunctionDef(
    name="matches",
    category="string",
    args=["regex"],
    description="Returns true if the entire subject matches the given Java regex.",
    pyspark_col="{subject}.rlike('^' + {arg0} + '$')",
    pyspark_python="bool(re.fullmatch({arg0}, {subject}))",
    notes="NiFi matches() requires a full match; PySpark rlike is a partial match, so anchors are added.",
))

_register(NELFunctionDef(
    name="find",
    category="string",
    args=["regex"],
    description="Returns true if any part of the subject matches the given regex.",
    pyspark_col="{subject}.rlike({arg0})",
    pyspark_python="bool(re.search({arg0}, {subject}))",
))

_register(NELFunctionDef(
    name="indexOf",
    category="string",
    args=["value"],
    description="Returns the 0-based index of the first occurrence of *value*, or -1.",
    pyspark_col="F.locate({arg0}, {subject}) - 1",
    pyspark_python="{subject}.find({arg0})",
    notes="PySpark locate() is 1-based; subtract 1 to match NiFi's 0-based convention.",
))

_register(NELFunctionDef(
    name="lastIndexOf",
    category="string",
    args=["value"],
    description="Returns the 0-based index of the last occurrence of *value*, or -1.",
    pyspark_col="F.length({subject}) - F.locate(F.reverse({arg0}), F.reverse({subject})) - F.length({arg0}) + 1",
    pyspark_python="{subject}.rfind({arg0})",
    notes="No native PySpark lastIndexOf; a reverse-locate trick or UDF is used.",
))

_register(NELFunctionDef(
    name="split",
    category="string",
    args=["delimiter"],
    description="Splits the subject string around *delimiter* and returns an array.",
    pyspark_col="F.split({subject}, {arg0})",
    pyspark_python="{subject}.split({arg0})",
))

_register(NELFunctionDef(
    name="padLeft",
    category="string",
    args=["desiredLength", "padChar"],
    description="Left-pads the subject to *desiredLength* using *padChar*.",
    pyspark_col="F.lpad({subject}, {arg0}, {arg1})",
    pyspark_python="{subject}.rjust({arg0}, {arg1})",
))

_register(NELFunctionDef(
    name="padRight",
    category="string",
    args=["desiredLength", "padChar"],
    description="Right-pads the subject to *desiredLength* using *padChar*.",
    pyspark_col="F.rpad({subject}, {arg0}, {arg1})",
    pyspark_python="{subject}.ljust({arg0}, {arg1})",
))

_register(NELFunctionDef(
    name="urlEncode",
    category="string",
    args=[],
    description="URL-encodes the subject string.",
    pyspark_col="F.expr(\"reflect('java.net.URLEncoder', 'encode', \" + {subject} + \", 'UTF-8')\")",
    pyspark_python="urllib.parse.quote({subject}, safe='')",
    notes="PySpark uses Java reflection for URL encoding; a UDF is often cleaner.",
))

_register(NELFunctionDef(
    name="urlDecode",
    category="string",
    args=[],
    description="URL-decodes the subject string.",
    pyspark_col="F.expr(\"reflect('java.net.URLDecoder', 'decode', \" + {subject} + \", 'UTF-8')\")",
    pyspark_python="urllib.parse.unquote({subject})",
))

_register(NELFunctionDef(
    name="escapeJson",
    category="string",
    args=[],
    description="Escapes characters in the subject for use in a JSON string.",
    pyspark_col="F.regexp_replace({subject}, '([\"\\\\\\\\])', '\\\\\\\\$1')",
    pyspark_python="json.dumps({subject})[1:-1]",
    notes="Approximate; a UDF may be needed for full RFC compliance.",
))

_register(NELFunctionDef(
    name="unescapeJson",
    category="string",
    args=[],
    description="Unescapes a JSON-escaped subject string.",
    pyspark_col="F.regexp_replace({subject}, '\\\\\\\\([\"\\\\\\\\])', '$1')",
    pyspark_python="json.loads('\"' + {subject} + '\"')",
))

_register(NELFunctionDef(
    name="escapeHtml3",
    category="string",
    args=[],
    description="Escapes the subject for safe inclusion in HTML 3.2 output.",
    pyspark_col="F.expr(\"reflect('org.apache.commons.text.StringEscapeUtils', 'escapeHtml3', \" + {subject} + \")\")",
    pyspark_python="html.escape({subject})",
    notes="PySpark: Java reflection or UDF.  Python html.escape covers the common entities.",
))

_register(NELFunctionDef(
    name="escapeHtml4",
    category="string",
    args=[],
    description="Escapes the subject for safe inclusion in HTML 4.0 output.",
    pyspark_col="F.expr(\"reflect('org.apache.commons.text.StringEscapeUtils', 'escapeHtml4', \" + {subject} + \")\")",
    pyspark_python="html.escape({subject})",
))

_register(NELFunctionDef(
    name="escapeXml",
    category="string",
    args=[],
    description="Escapes the subject for safe inclusion in XML.",
    pyspark_col="F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace({subject}, '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), '\"', '&quot;'), \"'\", '&apos;')",
    pyspark_python="xml.sax.saxutils.escape({subject})",
))

_register(NELFunctionDef(
    name="unescapeXml",
    category="string",
    args=[],
    description="Unescapes an XML-escaped subject string.",
    pyspark_col="F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace({subject}, '&amp;', '&'), '&lt;', '<'), '&gt;', '>'), '&quot;', '\"'), '&apos;', \"'\")",
    pyspark_python="xml.sax.saxutils.unescape({subject})",
))

_register(NELFunctionDef(
    name="escapeCsv",
    category="string",
    args=[],
    description="Escapes the subject for safe inclusion as a CSV field.",
    pyspark_col="F.when({subject}.contains(',') | {subject}.contains('\"') | {subject}.contains('\\n'), F.concat(F.lit('\"'), F.regexp_replace({subject}, '\"', '\"\"'), F.lit('\"'))).otherwise({subject})",
    pyspark_python="'\"' + {subject}.replace('\"', '\"\"') + '\"' if any(c in {subject} for c in [',', '\"', '\\n']) else {subject}",
))

_register(NELFunctionDef(
    name="jsonPath",
    category="string",
    args=["path"],
    description="Evaluates a JSONPath expression against the subject JSON string.",
    pyspark_col="F.get_json_object({subject}, {arg0})",
    pyspark_python="jsonpath_ng.parse({arg0}).find(json.loads({subject}))[0].value",
))

_register(NELFunctionDef(
    name="jsonPathDelete",
    category="string",
    args=["path"],
    description="Deletes the value at the given JSONPath from the subject JSON string.",
    pyspark_col="# UDF required: json_path_delete({subject}, {arg0})",
    pyspark_python="# use jsonpath_ng to delete at {arg0}",
    notes="No native PySpark equivalent; requires a UDF that parses JSON, removes the key, and re-serializes.",
))

_register(NELFunctionDef(
    name="jsonPathSet",
    category="string",
    args=["path", "value"],
    description="Sets the value at the given JSONPath in the subject JSON string.",
    pyspark_col="# UDF required: json_path_set({subject}, {arg0}, {arg1})",
    pyspark_python="# use jsonpath_ng to set {arg0} = {arg1}",
    notes="No native PySpark equivalent; requires a UDF.",
))

_register(NELFunctionDef(
    name="jsonPathAdd",
    category="string",
    args=["path", "value"],
    description="Appends a value to an array at the given JSONPath in the subject JSON string.",
    pyspark_col="# UDF required: json_path_add({subject}, {arg0}, {arg1})",
    pyspark_python="# use jsonpath_ng to append {arg1} at {arg0}",
    notes="No native PySpark equivalent; requires a UDF.",
))

# ===================================================================
# MATH FUNCTIONS  (23 entries)
# ===================================================================

_register(NELFunctionDef(
    name="plus",
    category="math",
    args=["operand"],
    description="Adds *operand* to the subject.",
    pyspark_col="{subject} + {arg0}",
    pyspark_python="{subject} + {arg0}",
))

_register(NELFunctionDef(
    name="minus",
    category="math",
    args=["operand"],
    description="Subtracts *operand* from the subject.",
    pyspark_col="{subject} - {arg0}",
    pyspark_python="{subject} - {arg0}",
))

_register(NELFunctionDef(
    name="multiply",
    category="math",
    args=["operand"],
    description="Multiplies the subject by *operand*.",
    pyspark_col="{subject} * {arg0}",
    pyspark_python="{subject} * {arg0}",
))

_register(NELFunctionDef(
    name="divide",
    category="math",
    args=["operand"],
    description="Divides the subject by *operand*.",
    pyspark_col="{subject} / {arg0}",
    pyspark_python="{subject} / {arg0}",
))

_register(NELFunctionDef(
    name="mod",
    category="math",
    args=["operand"],
    description="Returns the subject modulo *operand*.",
    pyspark_col="{subject} % {arg0}",
    pyspark_python="{subject} % {arg0}",
))

_register(NELFunctionDef(
    name="toNumber",
    category="math",
    args=[],
    description="Converts the subject string to a numeric type (Long or Double).",
    pyspark_col="{subject}.cast('double')",
    pyspark_python="float({subject})",
    notes="Also used in type_conversion and date categories with different semantics.",
))

_register(NELFunctionDef(
    name="gt",
    category="math",
    args=["operand"],
    description="Returns true if the subject is greater than *operand*.",
    pyspark_col="{subject} > {arg0}",
    pyspark_python="{subject} > {arg0}",
))

_register(NELFunctionDef(
    name="ge",
    category="math",
    args=["operand"],
    description="Returns true if the subject is greater than or equal to *operand*.",
    pyspark_col="{subject} >= {arg0}",
    pyspark_python="{subject} >= {arg0}",
))

_register(NELFunctionDef(
    name="lt",
    category="math",
    args=["operand"],
    description="Returns true if the subject is less than *operand*.",
    pyspark_col="{subject} < {arg0}",
    pyspark_python="{subject} < {arg0}",
))

_register(NELFunctionDef(
    name="le",
    category="math",
    args=["operand"],
    description="Returns true if the subject is less than or equal to *operand*.",
    pyspark_col="{subject} <= {arg0}",
    pyspark_python="{subject} <= {arg0}",
))

_register(NELFunctionDef(
    name="math:abs",
    category="math",
    args=[],
    description="Returns the absolute value of the subject.",
    pyspark_col="F.abs({subject})",
    pyspark_python="abs({subject})",
))

_register(NELFunctionDef(
    name="math:ceil",
    category="math",
    args=[],
    description="Returns the smallest integer greater than or equal to the subject.",
    pyspark_col="F.ceil({subject})",
    pyspark_python="math.ceil({subject})",
))

_register(NELFunctionDef(
    name="math:floor",
    category="math",
    args=[],
    description="Returns the largest integer less than or equal to the subject.",
    pyspark_col="F.floor({subject})",
    pyspark_python="math.floor({subject})",
))

_register(NELFunctionDef(
    name="math:round",
    category="math",
    args=[],
    description="Rounds the subject to the nearest integer.",
    pyspark_col="F.round({subject})",
    pyspark_python="round({subject})",
))

_register(NELFunctionDef(
    name="math:sqrt",
    category="math",
    args=[],
    description="Returns the square root of the subject.",
    pyspark_col="F.sqrt({subject})",
    pyspark_python="math.sqrt({subject})",
))

_register(NELFunctionDef(
    name="math:log",
    category="math",
    args=[],
    description="Returns the natural logarithm (base e) of the subject.",
    pyspark_col="F.log({subject})",
    pyspark_python="math.log({subject})",
))

_register(NELFunctionDef(
    name="math:log10",
    category="math",
    args=[],
    description="Returns the base-10 logarithm of the subject.",
    pyspark_col="F.log10({subject})",
    pyspark_python="math.log10({subject})",
))

_register(NELFunctionDef(
    name="math:pow",
    category="math",
    args=["exponent"],
    description="Returns the subject raised to the power of *exponent*.",
    pyspark_col="F.pow({subject}, {arg0})",
    pyspark_python="math.pow({subject}, {arg0})",
))

_register(NELFunctionDef(
    name="math:random",
    category="math",
    args=[],
    description="Returns a random double between 0.0 (inclusive) and 1.0 (exclusive).",
    pyspark_col="F.rand()",
    pyspark_python="random.random()",
    notes="Subject is ignored; the function is a static generator.",
))

_register(NELFunctionDef(
    name="math:min",
    category="math",
    args=["operand"],
    description="Returns the lesser of the subject and *operand*.",
    pyspark_col="F.least({subject}, {arg0})",
    pyspark_python="min({subject}, {arg0})",
))

_register(NELFunctionDef(
    name="math:max",
    category="math",
    args=["operand"],
    description="Returns the greater of the subject and *operand*.",
    pyspark_col="F.greatest({subject}, {arg0})",
    pyspark_python="max({subject}, {arg0})",
))

_register(NELFunctionDef(
    name="toRadians",
    category="math",
    args=[],
    description="Converts the subject from degrees to radians.",
    pyspark_col="F.radians({subject})",
    pyspark_python="math.radians({subject})",
))

_register(NELFunctionDef(
    name="toDegrees",
    category="math",
    args=[],
    description="Converts the subject from radians to degrees.",
    pyspark_col="F.degrees({subject})",
    pyspark_python="math.degrees({subject})",
))

# ===================================================================
# DATE / TIME FUNCTIONS  (12 entries)
# ===================================================================

_register(NELFunctionDef(
    name="now",
    category="date",
    args=[],
    description="Returns the current date and time as a Date object.",
    pyspark_col="F.current_timestamp()",
    pyspark_python="datetime.datetime.now()",
    notes="NiFi returns a java.util.Date; PySpark returns a TimestampType column.",
))

_register(NELFunctionDef(
    name="format",
    category="date",
    args=["pattern"],
    description="Formats the subject Date using the given SimpleDateFormat pattern.",
    pyspark_col="F.date_format({subject}, {arg0})",
    pyspark_python="{subject}.strftime({arg0})",
    notes="NiFi uses Java SimpleDateFormat patterns (e.g. yyyy-MM-dd); PySpark date_format also uses Java patterns.  Python strftime uses %-codes.",
))

_register(NELFunctionDef(
    name="toDate",
    category="date",
    args=["pattern"],
    description="Parses the subject string into a Date using the given SimpleDateFormat pattern.",
    pyspark_col="F.to_timestamp({subject}, {arg0})",
    pyspark_python="datetime.datetime.strptime({subject}, {arg0})",
    notes="NiFi allows an optional timezone argument; PySpark to_timestamp uses the session timezone.",
))

_register(NELFunctionDef(
    name="getYear",
    category="date",
    args=[],
    description="Returns the four-digit year of the subject Date.",
    pyspark_col="F.year({subject})",
    pyspark_python="{subject}.year",
))

_register(NELFunctionDef(
    name="getMonth",
    category="date",
    args=[],
    description="Returns the month (1-12) of the subject Date.",
    pyspark_col="F.month({subject})",
    pyspark_python="{subject}.month",
))

_register(NELFunctionDef(
    name="getDayOfMonth",
    category="date",
    args=[],
    description="Returns the day of the month (1-31) of the subject Date.",
    pyspark_col="F.dayofmonth({subject})",
    pyspark_python="{subject}.day",
))

_register(NELFunctionDef(
    name="getDayOfWeek",
    category="date",
    args=[],
    description="Returns the day of the week (1=Sunday ... 7=Saturday) of the subject Date.",
    pyspark_col="F.dayofweek({subject})",
    pyspark_python="{subject}.isoweekday() % 7 + 1",
    notes="NiFi follows java.util.Calendar convention (1=Sunday); PySpark dayofweek also returns 1=Sunday.",
))

_register(NELFunctionDef(
    name="getDayOfYear",
    category="date",
    args=[],
    description="Returns the day of the year (1-366) of the subject Date.",
    pyspark_col="F.dayofyear({subject})",
    pyspark_python="{subject}.timetuple().tm_yday",
))

_register(NELFunctionDef(
    name="getHour",
    category="date",
    args=[],
    description="Returns the hour (0-23) of the subject Date.",
    pyspark_col="F.hour({subject})",
    pyspark_python="{subject}.hour",
))

_register(NELFunctionDef(
    name="getMinute",
    category="date",
    args=[],
    description="Returns the minute (0-59) of the subject Date.",
    pyspark_col="F.minute({subject})",
    pyspark_python="{subject}.minute",
))

_register(NELFunctionDef(
    name="getSecond",
    category="date",
    args=[],
    description="Returns the second (0-59) of the subject Date.",
    pyspark_col="F.second({subject})",
    pyspark_python="{subject}.second",
))

_register(NELFunctionDef(
    name="toNumber_date",
    category="date",
    args=[],
    description="Converts the subject Date to epoch milliseconds (Long).",
    pyspark_col="(F.unix_timestamp({subject}) * 1000).cast('long')",
    pyspark_python="int({subject}.timestamp() * 1000)",
    notes="Registered under a distinct key to avoid collision with the math toNumber.  At code-gen time choose by context (date vs string).",
))

# ===================================================================
# BOOLEAN / LOGIC FUNCTIONS  (14 entries)
# ===================================================================

_register(NELFunctionDef(
    name="isNull",
    category="boolean",
    args=[],
    description="Returns true if the subject is null.",
    pyspark_col="{subject}.isNull()",
    pyspark_python="{subject} is None",
))

_register(NELFunctionDef(
    name="notNull",
    category="boolean",
    args=[],
    description="Returns true if the subject is not null.",
    pyspark_col="{subject}.isNotNull()",
    pyspark_python="{subject} is not None",
))

_register(NELFunctionDef(
    name="isEmpty",
    category="boolean",
    args=[],
    description="Returns true if the subject is null or has zero length.",
    pyspark_col="({subject}.isNull()) | (F.length({subject}) == 0)",
    pyspark_python="not {subject}",
))

_register(NELFunctionDef(
    name="isBlank",
    category="boolean",
    args=[],
    description="Returns true if the subject is null, empty, or contains only whitespace.",
    pyspark_col="({subject}.isNull()) | (F.length(F.trim({subject})) == 0)",
    pyspark_python="not ({subject} and {subject}.strip())",
))

_register(NELFunctionDef(
    name="not",
    category="boolean",
    args=[],
    description="Negates the boolean subject.",
    pyspark_col="~{subject}",
    pyspark_python="not {subject}",
))

_register(NELFunctionDef(
    name="and",
    category="boolean",
    args=["operand"],
    description="Logical AND of the subject and *operand*.",
    pyspark_col="{subject} & {arg0}",
    pyspark_python="{subject} and {arg0}",
))

_register(NELFunctionDef(
    name="or",
    category="boolean",
    args=["operand"],
    description="Logical OR of the subject and *operand*.",
    pyspark_col="{subject} | {arg0}",
    pyspark_python="{subject} or {arg0}",
))

_register(NELFunctionDef(
    name="ifElse",
    category="boolean",
    args=["trueValue", "falseValue"],
    description="Returns *trueValue* if the subject is true, otherwise *falseValue*.",
    pyspark_col="F.when({subject}, F.lit({arg0})).otherwise(F.lit({arg1}))",
    pyspark_python="{arg0} if {subject} else {arg1}",
))

_register(NELFunctionDef(
    name="in",
    category="boolean",
    args=["value1", "value2"],
    description="Returns true if the subject equals any of the supplied values.",
    pyspark_col="{subject}.isin([{arg0}, {arg1}])",
    pyspark_python="{subject} in [{arg0}, {arg1}]",
    notes="NiFi in() accepts a variable number of arguments; the generated isin() list is expanded at code-gen time.",
))

_register(NELFunctionDef(
    name="literal",
    category="boolean",
    args=["value"],
    description="Returns the literal value *value* regardless of the subject.",
    pyspark_col="F.lit({arg0})",
    pyspark_python="{arg0}",
    notes="Often used as ${literal('some_value'):...} to start an expression chain without an attribute.",
))

_register(NELFunctionDef(
    name="isNumber",
    category="boolean",
    args=[],
    description="Returns true if the subject can be parsed as a number.",
    pyspark_col="{subject}.cast('double').isNotNull()",
    pyspark_python="{subject}.replace('.','',1).replace('-','',1).isdigit()",
))

_register(NELFunctionDef(
    name="count",
    category="boolean",
    args=[],
    description="Returns the number of non-null values when used with multi-value functions.",
    pyspark_col="F.count(F.when({subject}.isNotNull(), {subject}))",
    pyspark_python="sum(1 for v in {subject} if v is not None)",
    notes="Context-dependent: in multi-value expressions, operates over the attribute set.",
))

_register(NELFunctionDef(
    name="join",
    category="boolean",
    args=["delimiter"],
    description="Joins multi-valued results with *delimiter*.",
    pyspark_col="F.concat_ws({arg0}, {subject})",
    pyspark_python="{arg0}.join({subject})",
    notes="Typically used after anyAttribute / allAttributes to combine results.",
))

_register(NELFunctionDef(
    name="returnType_boolean",
    category="boolean",
    args=[],
    description="Ensures the expression returns a boolean.  Used internally by NiFi.",
    pyspark_col="{subject}.cast('boolean')",
    pyspark_python="bool({subject})",
    notes="Rarely seen in user expressions; included for completeness.",
))

# ===================================================================
# ENCODING FUNCTIONS  (10 entries)
# ===================================================================

_register(NELFunctionDef(
    name="base64Encode",
    category="encoding",
    args=[],
    description="Base64-encodes the subject bytes or string.",
    pyspark_col="F.base64(F.encode({subject}, 'UTF-8'))",
    pyspark_python="base64.b64encode({subject}.encode('utf-8')).decode('ascii')",
))

_register(NELFunctionDef(
    name="base64Decode",
    category="encoding",
    args=[],
    description="Decodes the Base64-encoded subject back to a string.",
    pyspark_col="F.decode(F.unbase64({subject}), 'UTF-8')",
    pyspark_python="base64.b64decode({subject}).decode('utf-8')",
))

_register(NELFunctionDef(
    name="hash",
    category="encoding",
    args=["algorithm"],
    description="Hashes the subject using the given algorithm (MD5, SHA-256, SHA-512, etc.).",
    pyspark_col="F.sha2({subject}, 256)",
    pyspark_python="hashlib.sha256({subject}.encode()).hexdigest()",
    notes="PySpark sha2 supports 224, 256, 384, 512.  For MD5 use F.md5().  The algorithm argument selects the variant.",
))

_register(NELFunctionDef(
    name="hash_md5",
    category="encoding",
    args=[],
    description="Computes the MD5 hash of the subject.",
    pyspark_col="F.md5({subject})",
    pyspark_python="hashlib.md5({subject}.encode()).hexdigest()",
    notes="Convenience alias when the NiFi expression uses hash('MD5').",
))

_register(NELFunctionDef(
    name="hash_sha1",
    category="encoding",
    args=[],
    description="Computes the SHA-1 hash of the subject.",
    pyspark_col="F.sha1({subject})",
    pyspark_python="hashlib.sha1({subject}.encode()).hexdigest()",
    notes="Convenience alias when the NiFi expression uses hash('SHA-1').",
))

_register(NELFunctionDef(
    name="hash_sha256",
    category="encoding",
    args=[],
    description="Computes the SHA-256 hash of the subject.",
    pyspark_col="F.sha2({subject}, 256)",
    pyspark_python="hashlib.sha256({subject}.encode()).hexdigest()",
))

_register(NELFunctionDef(
    name="hash_sha512",
    category="encoding",
    args=[],
    description="Computes the SHA-512 hash of the subject.",
    pyspark_col="F.sha2({subject}, 512)",
    pyspark_python="hashlib.sha512({subject}.encode()).hexdigest()",
))

_register(NELFunctionDef(
    name="UUID",
    category="encoding",
    args=[],
    description="Generates a random Type 4 UUID.",
    pyspark_col="F.expr('uuid()')",
    pyspark_python="str(uuid.uuid4())",
    notes="Also categorized under 'system'.  NiFi returns a UUID string.",
))

_register(NELFunctionDef(
    name="UUID3",
    category="encoding",
    args=["namespace", "name"],
    description="Generates a Type 3 (MD5-based) UUID from a namespace and name.",
    pyspark_col="F.md5(F.concat(F.lit({arg0}), {subject}))",
    pyspark_python="str(uuid.uuid3(uuid.UUID({arg0}), {arg1}))",
    notes="PySpark approximation; for strict RFC 4122 compliance use a UDF.",
))

_register(NELFunctionDef(
    name="UUID5",
    category="encoding",
    args=["namespace", "name"],
    description="Generates a Type 5 (SHA-1-based) UUID from a namespace and name.",
    pyspark_col="F.sha1(F.concat(F.lit({arg0}), {subject}))",
    pyspark_python="str(uuid.uuid5(uuid.UUID({arg0}), {arg1}))",
    notes="PySpark approximation; for strict RFC 4122 compliance use a UDF.",
))

# ===================================================================
# SYSTEM FUNCTIONS  (7 entries)
# ===================================================================

# 'now' is already registered under date; duplicate reference here
_register(NELFunctionDef(
    name="system:now",
    category="system",
    args=[],
    description="Alias for now() -- returns the current timestamp.",
    pyspark_col="F.current_timestamp()",
    pyspark_python="datetime.datetime.now()",
))

# 'UUID' is already registered under encoding; duplicate reference here
_register(NELFunctionDef(
    name="system:UUID",
    category="system",
    args=[],
    description="Alias for UUID() -- generates a random Type 4 UUID.",
    pyspark_col="F.expr('uuid()')",
    pyspark_python="str(uuid.uuid4())",
))

_register(NELFunctionDef(
    name="hostname",
    category="system",
    args=[],
    description="Returns the hostname of the machine running the NiFi instance.",
    pyspark_col="F.lit('databricks-node')",
    pyspark_python="socket.gethostname()",
    notes="In Databricks the hostname is typically irrelevant; replaced with a placeholder literal.",
))

_register(NELFunctionDef(
    name="ip",
    category="system",
    args=[],
    description="Returns the IP address of the NiFi instance.",
    pyspark_col="F.lit('0.0.0.0')",
    pyspark_python="socket.gethostbyname(socket.gethostname())",
    notes="In Databricks the NiFi IP is meaningless; replaced with a placeholder.",
))

_register(NELFunctionDef(
    name="nextInt",
    category="system",
    args=[],
    description="Returns a one-up counter scoped to the calling component (auto-increment).",
    pyspark_col="F.monotonically_increasing_id()",
    pyspark_python="next(_counter_iterator)",
    notes="PySpark monotonically_increasing_id() is unique but not sequential.",
))

_register(NELFunctionDef(
    name="thread",
    category="system",
    args=[],
    description="Returns the name of the current Java thread.  Informational only.",
    pyspark_col="F.lit('spark-executor')",
    pyspark_python="threading.current_thread().name",
    notes="Only useful for debugging in NiFi; replaced with a constant in Databricks.",
))

_register(NELFunctionDef(
    name="getStateValue",
    category="system",
    args=["key"],
    description="Retrieves a value from the processor's state map by key.",
    pyspark_col="# No direct equivalent -- use Delta table state or widget",
    pyspark_python="# spark.conf.get({arg0}) or Delta checkpoint",
    notes="NiFi processor state has no direct Databricks analog; use Spark conf, Delta checkpoints, or dbutils widgets.",
))

# ===================================================================
# MULTI-VALUE FUNCTIONS  (10 entries)
# ===================================================================

_register(NELFunctionDef(
    name="anyAttribute",
    category="multi_value",
    args=["attr1", "attr2"],
    description="Evaluates the rest of the expression against each listed attribute and returns true if any match.",
    pyspark_col="F.coalesce({arg0}, {arg1})",
    pyspark_python="any([{arg0}, {arg1}])",
    notes="Argument list is variable-length.  Code generator should expand to include all named attributes.",
))

_register(NELFunctionDef(
    name="allAttributes",
    category="multi_value",
    args=["attr1", "attr2"],
    description="Evaluates the rest of the expression against each listed attribute and returns true only if all match.",
    pyspark_col="F.when(({arg0}.isNotNull()) & ({arg1}.isNotNull()), F.lit(True)).otherwise(F.lit(False))",
    pyspark_python="all([{arg0}, {arg1}])",
    notes="Argument list is variable-length.",
))

_register(NELFunctionDef(
    name="anyMatchingAttribute",
    category="multi_value",
    args=["regex"],
    description="Like anyAttribute but selects attributes whose names match the given regex.",
    pyspark_col="# Dynamic column selection by regex -- use df.select([c for c in df.columns if re.search({arg0}, c)])",
    pyspark_python="any(v for k, v in attrs.items() if re.search({arg0}, k))",
    notes="Requires runtime column introspection; typically handled at code-gen time.",
))

_register(NELFunctionDef(
    name="allMatchingAttributes",
    category="multi_value",
    args=["regex"],
    description="Like allAttributes but selects attributes whose names match the given regex.",
    pyspark_col="# Dynamic column selection by regex -- use df.select([c for c in df.columns if re.search({arg0}, c)])",
    pyspark_python="all(v for k, v in attrs.items() if re.search({arg0}, k))",
))

_register(NELFunctionDef(
    name="anyDelineatedValue",
    category="multi_value",
    args=["delimiter"],
    description="Splits the subject on *delimiter* and returns true if any resulting value satisfies the rest of the expression.",
    pyspark_col="F.exists(F.split({subject}, {arg0}), lambda x: x.isNotNull())",
    pyspark_python="any({subject}.split({arg0}))",
))

_register(NELFunctionDef(
    name="allDelineatedValues",
    category="multi_value",
    args=["delimiter"],
    description="Splits the subject on *delimiter* and returns true only if every resulting value satisfies the rest of the expression.",
    pyspark_col="F.forall(F.split({subject}, {arg0}), lambda x: x.isNotNull())",
    pyspark_python="all({subject}.split({arg0}))",
))

_register(NELFunctionDef(
    name="count_multi",
    category="multi_value",
    args=[],
    description="Returns the number of values in a multi-value result set.",
    pyspark_col="F.size({subject})",
    pyspark_python="len({subject})",
    notes="Used after anyAttribute / allAttributes / delineated value functions.",
))

_register(NELFunctionDef(
    name="join_multi",
    category="multi_value",
    args=["delimiter"],
    description="Joins multi-valued results with *delimiter*.",
    pyspark_col="F.concat_ws({arg0}, {subject})",
    pyspark_python="{arg0}.join({subject})",
))

_register(NELFunctionDef(
    name="getDelimitedField",
    category="multi_value",
    args=["index", "delimiter"],
    description="Returns the Nth (1-based) field when the subject is split by *delimiter*.",
    pyspark_col="F.element_at(F.split({subject}, {arg1}), {arg0})",
    pyspark_python="{subject}.split({arg1})[{arg0} - 1]",
    notes="NiFi uses 1-based indexing; Python slicing is 0-based.",
))

_register(NELFunctionDef(
    name="getDelimitedFieldCount",
    category="multi_value",
    args=["delimiter"],
    description="Returns the number of fields when the subject is split by *delimiter*.",
    pyspark_col="F.size(F.split({subject}, {arg0}))",
    pyspark_python="len({subject}.split({arg0}))",
))

# ===================================================================
# TYPE CONVERSION FUNCTIONS  (6 entries)
# ===================================================================

_register(NELFunctionDef(
    name="toString",
    category="type_conversion",
    args=[],
    description="Converts the subject to its string representation.",
    pyspark_col="{subject}.cast('string')",
    pyspark_python="str({subject})",
))

_register(NELFunctionDef(
    name="toNumber_conv",
    category="type_conversion",
    args=[],
    description="Converts the subject string to a numeric type (Long or Double).",
    pyspark_col="{subject}.cast('double')",
    pyspark_python="float({subject})",
    notes="Same semantics as math:toNumber but registered under type_conversion for classification.",
))

_register(NELFunctionDef(
    name="toDate_conv",
    category="type_conversion",
    args=["pattern"],
    description="Parses the subject string into a Date using the given pattern.",
    pyspark_col="F.to_timestamp({subject}, {arg0})",
    pyspark_python="datetime.datetime.strptime({subject}, {arg0})",
    notes="Same semantics as date:toDate but registered under type_conversion.",
))

_register(NELFunctionDef(
    name="toRadians_conv",
    category="type_conversion",
    args=[],
    description="Converts the subject from degrees to radians.",
    pyspark_col="F.radians({subject})",
    pyspark_python="math.radians({subject})",
))

_register(NELFunctionDef(
    name="toDegrees_conv",
    category="type_conversion",
    args=[],
    description="Converts the subject from radians to degrees.",
    pyspark_col="F.degrees({subject})",
    pyspark_python="math.degrees({subject})",
))

_register(NELFunctionDef(
    name="toLong",
    category="type_conversion",
    args=[],
    description="Converts the subject to a Long integer.",
    pyspark_col="{subject}.cast('long')",
    pyspark_python="int({subject})",
))


# ===================================================================
# LOOKUP / HELPER FUNCTIONS
# ===================================================================

def lookup_nel_function(name: str) -> Optional[NELFunctionDef]:
    """Look up a single NEL function definition by name.

    Performs an exact match first, then falls back to a case-insensitive
    search, and finally checks for common aliases (e.g. ``toUpper`` matches
    ``toUpperCase``).

    Parameters
    ----------
    name:
        The NiFi Expression Language function name.

    Returns
    -------
    NELFunctionDef | None
        The matching definition, or ``None`` if not found.
    """
    # Exact match
    if name in NEL_FUNCTION_KB:
        return NEL_FUNCTION_KB[name]

    # Case-insensitive match
    name_lower = name.lower()
    for key, fn_def in NEL_FUNCTION_KB.items():
        if key.lower() == name_lower:
            return fn_def

    # Common aliases
    _ALIASES: dict[str, str] = {
        "toUpperCase": "toUpper",
        "toLowerCase": "toLower",
        "abs": "math:abs",
        "ceil": "math:ceil",
        "ceiling": "math:ceil",
        "floor": "math:floor",
        "round": "math:round",
        "sqrt": "math:sqrt",
        "log": "math:log",
        "log10": "math:log10",
        "pow": "math:pow",
        "power": "math:pow",
        "random": "math:random",
        "min": "math:min",
        "max": "math:max",
        "uuid": "UUID",
        "uuid4": "UUID",
        "base64encode": "base64Encode",
        "base64decode": "base64Decode",
        "md5": "hash_md5",
        "sha1": "hash_sha1",
        "sha256": "hash_sha256",
        "sha512": "hash_sha512",
        "getDelimitedValue": "getDelimitedField",
    }
    canonical = _ALIASES.get(name) or _ALIASES.get(name_lower)
    if canonical and canonical in NEL_FUNCTION_KB:
        return NEL_FUNCTION_KB[canonical]

    return None


def get_all_nel_functions() -> dict[str, NELFunctionDef]:
    """Return the entire NEL function knowledge base (defensive copy).

    Returns
    -------
    dict[str, NELFunctionDef]
        A shallow copy of the knowledge-base dictionary keyed by function name.
    """
    return dict(NEL_FUNCTION_KB)


def get_nel_functions_by_category(category: str) -> list[NELFunctionDef]:
    """Return all NEL functions belonging to *category*.

    Parameters
    ----------
    category:
        One of: ``string``, ``math``, ``date``, ``boolean``, ``encoding``,
        ``system``, ``multi_value``, ``type_conversion``.

    Returns
    -------
    list[NELFunctionDef]
        Matching definitions sorted by function name.
    """
    return sorted(
        [fn for fn in NEL_FUNCTION_KB.values() if fn.category == category],
        key=lambda fn: fn.name,
    )


def get_nel_categories() -> list[str]:
    """Return a sorted list of all distinct categories present in the KB.

    Returns
    -------
    list[str]
        Distinct category names in alphabetical order.
    """
    return sorted({fn.category for fn in NEL_FUNCTION_KB.values()})


def search_nel_functions(query: str) -> list[NELFunctionDef]:
    """Full-text search across function names, descriptions, and notes.

    Parameters
    ----------
    query:
        Case-insensitive search term.

    Returns
    -------
    list[NELFunctionDef]
        All matching definitions, sorted by name.
    """
    query_lower = query.lower()
    results = []
    for fn in NEL_FUNCTION_KB.values():
        if (query_lower in fn.name.lower()
                or query_lower in fn.description.lower()
                or query_lower in fn.notes.lower()
                or query_lower in fn.pyspark_col.lower()
                or query_lower in fn.pyspark_python.lower()):
            results.append(fn)
    return sorted(results, key=lambda fn: fn.name)
