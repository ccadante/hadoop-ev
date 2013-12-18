#bin/sh

echo ""
echo "exec \"bin/stop-all.sh\"..."
bin/stop-all.sh
echo "exec \"bin/stop-all.sh\" -- done"


echo ""
echo "exec \"bin/start-all.sh\"..."
bin/start-all.sh
echo "exec \"bin/start-all.sh\" -- done"

echo ""
echo "exec \"bin/start-all.sh\"..."
bin/hadoop dfsadmin -safemode leave
echo "exec \"bin/start-all.sh\" -- done"
