Expression: Operators
====

Operators are used to evaluate metrics in relation to eachother.

Operator Precedence
----

Operators are listed in descending order or precedence.

1.  ``()``  
   Associativity: left to right
2. ``! -``  
   Associativity: right to left  
   Note: the ``-`` character is the numeric negation, i.e. the dash in the expression ``-1``.
3. ``* / %``  
   Assiciativity: left to right
4. ``+ -``  
   Assiciativity: left to right  
   Note: the ``-`` character is the subtraction operation, i.e. the dash in the expression ``3 - 2 = 1``.
5. ``<< >>``  
   Assiciativity: left to right  
   Note: when applied to a floating point value, ``x << y`` will evaluate similar to ``x * 2^y``, while ``x >> y`` will evaluate similar to ``x / 2^y``.
6. ``< <= > >=``  
   Assiciativity: left to right
7. ``= !=``  
   Assiciativity: left to right
8. ``&&``  
   Assiciativity: left to right
9. ``||``  
   Assiciativity: left to right

Type Coercion
----

Operators will attempt to coerce their arguments to a valid type.  
If the operator operates on boolean values, it will attempt to coerce the value to a boolean.  
If the the operator operates on numeric value, it will attempt to coerce the value to a number.

Booleans and numeric types are freely coercible between eachother.
A numeric value coerced to a boolean will gain **true** if it is non-zero, otherwise it will gain the value **false**.
A boolean value coerced to a number will gain the value **1**, for **true**, or **0**, for **false**.
Strings cannot be coerced to another type.
