`
javaS112FDefine and throw a dedicated exception instead of using a generic one. 2tt &Ç
javaS2259FA "NullPointerException" could be thrown; "oldValue" is nullable here. 2
çç :á
,¬
çç 'oldValue' is dereferenced.
0¬
ää #Implies 'oldValue' can be null.
1¬
ää # 'deserialize()' can return null.
'¬UU 'IOException' is caught.
#¬SS EException is thrown.