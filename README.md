# StudentLife
Official github repo of w205 final project by Kevin Davis, Marek Sedlacek, and Minghu Song. 
##To run the application
0. Configure an ucbw205\_complete\_plus\_postgres\_PY2.7 m3.2xlarge instance with Spark1.5.2 as the default spark installation. Note that spark must be using the same python installation as pip. 
1. Download the main script at https://raw.githubusercontent.com/karzak/StudentLife/master/loading_commands.sh and save to your instance.
2. Make the script executable using chmod +x loading_commands.sh
3. Run the script using ./loading_commands.sh

All output files can be accessed at https://s3-us-west-2.amazonaws.com/edu.berkeley.ischool.test.w205.kjy/StudentLifeTables.zip
