#bin/sh

echo ""
echo "exec \"bin/stop-all.sh\"..."
bin/stop-all.sh
echo "exec \"bin/stop-all.sh\" -- done"

echo ""
echo "exec \"ant\"..."
ant
echo "exec \"ant\" -- done"

echo ""
echo "exec \"ant examples\"..."
ant examples
echo "exec \"ant examples\" -- done"

echo ""
echo "exec \"bin/start-all.sh\"..."
bin/start-all.sh
echo "exec \"bin/start-all.sh\" -- done"
