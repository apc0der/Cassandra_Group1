@ECHO OFF
>beans.txt (  
cd ../
pushd
call nodetool cfstats testkeyspace.bozos
)
cd C:\Users\TempUser\IdeaProjects\cassandrajank\src\main\java
javac Batchamus.java
call java Batchamus
popd
cd yeet
del "beans.txt"
@PAUSE