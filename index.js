// lambda function name: ffmpeg -i test.mov -hls_flags single_file out.m3u8
// option -hls_flags single_file

var async     = require('async')
    , AWS     = require('aws-sdk')
    , util    = require('util')
    , fs      = require('fs')
    , exec = require('child_process').exec;

var s3 = new AWS.S3();

exports.handler = function( event, context,callback ){

    console.log("S3 Content : ",util.inspect( event,{ depth:5}));
    var srcBucket = event.Records[0].s3.bucket.name ;
    var srcKey = event.Records[0].s3.object.key;

    var dstBucket ="delta-ffmpeg-hls";
    var dstKey    = srcKey.replace("mov", "m3u8");
    var dstDir    = srcKey.replace(".mov", "");

    if (srcBucket == dstBucket) {
        console.error("Destination bucket must not match source bucket.");
        return;
    }

    // Infer the video type.
    var typeMatch = srcKey.match(/\.([^.]*)$/);
    if (!typeMatch) {
        console.error('unable to infer image type for key ' + srcKey);
        return;
    }

    var validVideoTypes = ['mov', 'mp4'];
    var videoType = typeMatch[1];
    if (validVideoTypes.indexOf(videoType.toLowerCase()) < 0) {
        console.log('skipping non-vido ' + srcKey);
        return;
    }

    async.waterfall([
        function download(next) {

            var file = fs.createWriteStream('/tmp/'+srcKey);

            s3.getObject({
                Bucket : srcBucket,
                Key    : srcKey
            }).createReadStream()
            .on('error', (err)=>{
                console.log("S3 getObject error : "+err);
            }).pipe(file);

            file.on('finish', ()=>{
                exec('ls -al /tmp', (error, stdout, stderr)=> {
                    console.log("Get S3 object And Write file finished : "+stdout);
                });
                next()
            });
        },
        function main(next) {

            exec('./ffmpeg -benchmark -i /tmp/'+srcKey+' /tmp/'+dstKey, (error, stdout, stderr) =>{

                console.log('Command ffmpeg Stderr: '+stderr);
                console.log('Command ffmpeg Error: '+error);

                exec('ls -al /tmp', (error, stdout, stderr)=> {
                    console.log("Convert file 2 m3u8 : "+stdout);
                });

                fs.readdir('/tmp', (err, files) => {

                    async.eachSeries(files, function (file, callback) {
                    
                        fs.readFile('/tmp/'+file, { encoding: 'utf8' },  (err, data ) =>{    

                            if( err ){
                                console.log(err, err.stack); // an error occurred
                            }

                            var contentType = 'video/MP2T';
                            var extension = file.split('.').pop(); 
                            if( 'm3u8'==extension ) contentType= 'application/x-mpegURL';
                            if( 'mov'==extension ){
                                return;
                            }

                            var s3param = {
                                Bucket      : dstBucket,
                                Key         : dstDir+"/"+file,
                                Body        : data,
                                ContentType : contentType
                            };

                            s3.putObject(s3param, (err, data)=> {
                                if (err) console.log(err, err.stack); // an error occurred
                                else     console.log("s3 response :"+JSON.stringify(data));           // successful response
                            });
                        });
                        callback(); 

                    }, function (err) {
                        if (err) { throw err; }
                        console.log('Well done :-) !');
                        next()
                    });
                })
            });
        }],
        function (err) {
            if (err) {
                console.error(
                'Unable to transfer ' + srcBucket + '/' + srcKey +
                ' and upload to ' + dstBucket + '/' + dstKey +
                ' due to an error: ' + err
                );
                context.done();
            } else {

                exec('rm /tmp/* ; ls -al /tmp', (error, stdout, stderr)=> {
                    if( error ){
                        console.log(error, error.stack); // an error occurred
                    }
                    console.log("Remove all file delow tmp folder : "+stdout);
                });

                console.log(
                'Successfully transfer ' + srcBucket + '/' + srcKey +
                ' and uploaded to ' + dstBucket + '/' + dstKey
                );
            }
        }
    );
}