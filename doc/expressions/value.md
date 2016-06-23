Expression: Value
====

A value is a literal data unit.
No group or metric resolution is attempted to resolve it.

Values can be either:
- **boolean**
  The values **true** and **false** are the only boolean values that exist.
- **integral number**
  An integral number is a sequence of digits.
  Decimal numbers are the default unit.
  If the number starts with a zero (**0**), it will be considered an octal value.
  A hexadecimal number starts with **0x**.
- **floating point number**
  A floating point number uses a decimal separator and/or an exponent.
  Both decimal and hexadecimal notation are supported, the latter with the prefix **0x**.
  The exponent identifier is the **e** character in decimal mode, or the **p** number in hexadecimal mode, both lower case and upper case are allowed.
- **string value**
  A string value is enclosed in double quotes, like this: **"text"**.
  A string value can use escapes.
  Note that a string may not cross a line boundary (but you can add newlines by using the **\n** escape).

Syntax
----

- *Boolean values:*
  **true** | **false**
- *Integral values:*
  123...9 | **0** 123...7 | **0x** 012...9abcdef
- *Floating point values:*
  123 **.** 456 **e** 567 | **0x** 1 **.** fe **P** a0
- *String values:*
  **"** abc def **\n** **\t** **\u16ff** **\U00010fff** abc **\"** 99 **\'** **"**

Example
----

    true
    -9000

evaluates to a boolean representing the *true* value, followed by a constant with the value -9000.

    3.14159
    6.02214086e23

evaluate to an approximation of PI and 6.02214086 × 10<sup>23</sup> (Avogadro's constant) respectively.

    "A quick brown fox jumped over the lazy dog."

evaluates to a string type, holding the text *A quick brown fox jumped over the lazy dog.*

    "\u24b8"

evaluates to the Copyright character: &copy;.
