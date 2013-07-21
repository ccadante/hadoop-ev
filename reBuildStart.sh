#bin/sh

echo ""
echo "exec \"bin/stop-all.sh\"..."
bin/stop-all.sh
echo "exec \"bin/stop-all.sh\" -- done"

echo ""
echo "exec \"ant\"..."
ant
echo "exec \"ant\" -- done"

#echo ""
#echo "exec \"ant examples\"..."
#ant examples
#echo "exec \"ant examples\" -- done"

cd ./CombineOfflineMR
rm ./offline.jar
jar cvf offline.jar -C bin .
cd ..
echo "exec \"jar cvf offline.jar -C bin .\" -- done"

echo ""
echo "exec \"bin/start-all.sh\"..."
bin/start-all.sh
echo "exec \"bin/start-all.sh\" -- done"

echo ""
echo "exec \"bin/start-all.sh\"..."
bin/hadoop dfsadmin -safemode leave
echo "exec \"bin/start-all.sh\" -- done"
