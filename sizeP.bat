@ECHO OFF
>beans.txt (  
cd ../
pushd
call nodetool cfstats testkeyspace.videos
)
cd C:\Users\TempUser\IdeaProjects\cassandrajank\out\artifacts\cassandrajank_jar
java -jar cassandrajank.jar
popd
cd yeet
del "beans.txt"
@PAUSE