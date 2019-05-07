#import "GPUImageMovieWriter.h"

#import "GPUImageContext.h"
#import "GLProgram.h"
#import "GPUImageFilter.h"

NSString *const kGPUImageColorSwizzlingFragmentShaderString = SHADER_STRING
(
 varying highp vec2 textureCoordinate;

 uniform sampler2D inputImageTexture;

 void main()
 {
     gl_FragColor = texture2D(inputImageTexture, textureCoordinate).bgra;
 }
);


@interface GPUImageMovieWriter ()
{
    GLuint movieFramebuffer, movieRenderbuffer;

    GLProgram *colorSwizzlingProgram;
    GLint colorSwizzlingPositionAttribute, colorSwizzlingTextureCoordinateAttribute;
    GLint colorSwizzlingInputTextureUniform;

    GPUImageFramebuffer *firstInputFramebuffer;

    CMTime startTime, previousFrameTime, previousAudioTime;

    dispatch_queue_t audioQueue, videoQueue;
    BOOL audioEncodingIsFinished, videoEncodingIsFinished;

    BOOL isRecording;
}

// Movie recording
- (void)initializeMovieWithOutputSettings:(NSMutableDictionary *)outputSettings cameraFormat:(CMFormatDescriptionRef)camFmt;

// Frame rendering
- (void)createDataFBO;
- (void)destroyDataFBO;
- (void)setFilterFBO;

- (void)renderAtInternalSizeUsingFramebuffer:(GPUImageFramebuffer *)inputFramebufferToUse;

@end

@implementation GPUImageMovieWriter

@synthesize hasAudioTrack = _hasAudioTrack;
@synthesize encodingLiveVideo = _encodingLiveVideo;
@synthesize shouldPassthroughAudio = _shouldPassthroughAudio;
@synthesize completionBlock;
@synthesize failureBlock;
@synthesize videoInputReadyCallback;
@synthesize audioInputReadyCallback;
@synthesize enabled;
@synthesize shouldInvalidateAudioSampleWhenDone = _shouldInvalidateAudioSampleWhenDone;
@synthesize paused = _paused;
@synthesize movieWriterContext = _movieWriterContext;

@synthesize delegate = _delegate;

#pragma mark -
#pragma mark Initialization and teardown
- (id)initWithMovieURL:(NSURL *)newMovieURL size:(CGSize)newSize;
{
    return [self initWithMovieURL:newMovieURL size:newSize fileType:AVFileTypeQuickTimeMovie outputSettings:nil cameraFormat:nil];
}

- (id)initWithMovieURL:(NSURL *)newMovieURL size:(CGSize)newSize fileType:(NSString *)newFileType outputSettings:(NSMutableDictionary *)outputSettings cameraFormat:(CMFormatDescriptionRef)camFmt;
{
    if (!(self = [super init]))
    {
		return nil;
    }

    _shouldInvalidateAudioSampleWhenDone = NO;

    self.enabled = YES;
    alreadyFinishedRecording = NO;
    videoEncodingIsFinished = NO;
    audioEncodingIsFinished = NO;

    videoSize = newSize;
    movieURL = newMovieURL;
    fileType = newFileType;
    startTime = kCMTimeInvalid;
    _encodingLiveVideo = [[outputSettings objectForKey:@"EncodingLiveVideo"] isKindOfClass:[NSNumber class]] ? [[outputSettings objectForKey:@"EncodingLiveVideo"] boolValue] : YES;
    previousFrameTime = kCMTimeNegativeInfinity;
    previousAudioTime = kCMTimeNegativeInfinity;
    inputRotation = kGPUImageNoRotation;

    _movieWriterContext = [[GPUImageContext alloc] init];
    [_movieWriterContext useSharegroup:[[[GPUImageContext sharedImageProcessingContext] context] sharegroup]];

    runSynchronouslyOnContextQueue(_movieWriterContext, ^{
        [_movieWriterContext useAsCurrentContext];

        if ([GPUImageContext supportsFastTextureUpload])
        {
            colorSwizzlingProgram = [_movieWriterContext programForVertexShaderString:kGPUImageVertexShaderString fragmentShaderString:kGPUImagePassthroughFragmentShaderString];
        }
        else
        {
            colorSwizzlingProgram = [_movieWriterContext programForVertexShaderString:kGPUImageVertexShaderString fragmentShaderString:kGPUImageColorSwizzlingFragmentShaderString];
        }

        if (!colorSwizzlingProgram.initialized)
        {
            [colorSwizzlingProgram addAttribute:@"position"];
            [colorSwizzlingProgram addAttribute:@"inputTextureCoordinate"];

            if (![colorSwizzlingProgram link])
            {
                NSString *progLog = [colorSwizzlingProgram programLog];
                NSLog(@"Program link log: %@", progLog);
                NSString *fragLog = [colorSwizzlingProgram fragmentShaderLog];
                NSLog(@"Fragment shader compile log: %@", fragLog);
                NSString *vertLog = [colorSwizzlingProgram vertexShaderLog];
                NSLog(@"Vertex shader compile log: %@", vertLog);
                colorSwizzlingProgram = nil;
                NSAssert(NO, @"Filter shader link failed");
            }
        }

        colorSwizzlingPositionAttribute = [colorSwizzlingProgram attributeIndex:@"position"];
        colorSwizzlingTextureCoordinateAttribute = [colorSwizzlingProgram attributeIndex:@"inputTextureCoordinate"];
        colorSwizzlingInputTextureUniform = [colorSwizzlingProgram uniformIndex:@"inputImageTexture"];

        [_movieWriterContext setContextShaderProgram:colorSwizzlingProgram];

        glEnableVertexAttribArray(colorSwizzlingPositionAttribute);
        glEnableVertexAttribArray(colorSwizzlingTextureCoordinateAttribute);
    });

    [self initializeMovieWithOutputSettings:outputSettings cameraFormat:camFmt];

    return self;
}

- (void)dealloc;
{
    [self destroyDataFBO];

#if !OS_OBJECT_USE_OBJC
    if( audioQueue != NULL )
    {
        dispatch_release(audioQueue);
    }
    if( videoQueue != NULL )
    {
        dispatch_release(videoQueue);
    }
#endif
}

#pragma mark -
#pragma mark Movie recording

- (void)initializeMovieWithOutputSettings:(NSDictionary *)outputSettings cameraFormat:(CMFormatDescriptionRef)camFmt;
{
    isRecording = NO;

    self.enabled = YES;
    NSError *error = nil;
    assetWriter = [[AVAssetWriter alloc] initWithURL:movieURL fileType:fileType error:&error];
    if (error != nil)
    {
        NSLog(@"Error: %@", error);
        if (failureBlock)
        {
            failureBlock(error);
        }
        else
        {
            if(self.delegate && [self.delegate respondsToSelector:@selector(movieRecordingFailedWithError:)])
            {
                [self.delegate movieRecordingFailedWithError:error];
            }
        }
    }

    // Set this to make sure that a functional movie is produced, even if the recording is cut off mid-stream
    assetWriter.movieFragmentInterval = CMTimeMakeWithSeconds(1.0, 200);

    // use default output settings if none specified
    if (outputSettings == nil)
    {
        NSMutableDictionary *settings = [[NSMutableDictionary alloc] init];
        [settings setObject:AVVideoCodecH264 forKey:AVVideoCodecKey];
        [settings setObject:[NSNumber numberWithInt:videoSize.width] forKey:AVVideoWidthKey];
        [settings setObject:[NSNumber numberWithInt:videoSize.height] forKey:AVVideoHeightKey];
        outputSettings = settings;
    }
    // custom output settings specified
    else
    {
		NSString *videoCodec = [outputSettings objectForKey:AVVideoCodecKey];
		NSNumber *width = [outputSettings objectForKey:AVVideoWidthKey];
		NSNumber *height = [outputSettings objectForKey:AVVideoHeightKey];

		NSAssert(videoCodec && width && height, @"OutputSettings is missing required parameters.");

        if( [outputSettings objectForKey:@"EncodingLiveVideo"] ) {
            NSMutableDictionary *tmp = [outputSettings mutableCopy];
            [tmp removeObjectForKey:@"EncodingLiveVideo"];
            outputSettings = tmp;
        }
    }
    BOOL useDirectAppender = NO;
    if( [outputSettings objectForKey:@"DVGDirectAppend"] != nil) {
        useDirectAppender = [[outputSettings objectForKey:@"DVGDirectAppend"] integerValue]>0?YES:NO;
        NSMutableDictionary *tmp = [outputSettings mutableCopy];
        [tmp removeObjectForKey:@"DVGDirectAppend"];
        outputSettings = tmp;
    }


    if(camFmt != nil){
        assetWriterVideoInput = [AVAssetWriterInput assetWriterInputWithMediaType:AVMediaTypeVideo outputSettings:outputSettings sourceFormatHint:camFmt];
    }else{
        assetWriterVideoInput = [AVAssetWriterInput assetWriterInputWithMediaType:AVMediaTypeVideo outputSettings:outputSettings];
    }
    assetWriterVideoInput.expectsMediaDataInRealTime = _encodingLiveVideo;
    if(useDirectAppender == NO){
        // You need to use BGRA for the video in order to get realtime encoding. I use a color-swizzling shader to line up glReadPixels' normal RGBA output with the movie input's BGRA.
        NSDictionary *sourcePixelBufferAttributesDictionary = [NSDictionary dictionaryWithObjectsAndKeys:
                                                               [NSNumber numberWithInt:kCVPixelFormatType_32BGRA], kCVPixelBufferPixelFormatTypeKey,
                                                               [NSNumber numberWithInt:videoSize.width], kCVPixelBufferWidthKey,
                                                               [NSNumber numberWithInt:videoSize.height], kCVPixelBufferHeightKey,
                                                               @{(id)kCVImageBufferColorPrimariesKey: (id)kCVImageBufferColorPrimaries_ITU_R_709_2,
                                                                 (id)kCVImageBufferYCbCrMatrixKey: (id)kCVImageBufferYCbCrMatrix_ITU_R_601_4,
                                                                 (id)kCVImageBufferTransferFunctionKey: (id)kCVImageBufferTransferFunction_ITU_R_709_2}, kCVBufferPropagatedAttachmentsKey,
                                                               nil];
        assetWriterPixelBufferInput = [AVAssetWriterInputPixelBufferAdaptor assetWriterInputPixelBufferAdaptorWithAssetWriterInput:assetWriterVideoInput sourcePixelBufferAttributes:sourcePixelBufferAttributesDictionary];
    }
    [assetWriter addInput:assetWriterVideoInput];
}

- (void)setEncodingLiveVideo:(BOOL) value
{
    _encodingLiveVideo = value;
    if (isRecording) {
        NSAssert(NO, @"Can not change Encoding Live Video while recording");
    }
    else
    {
        assetWriterVideoInput.expectsMediaDataInRealTime = _encodingLiveVideo;
        assetWriterAudioInput.expectsMediaDataInRealTime = _encodingLiveVideo;
    }
}

- (void)startRecording;
{
    alreadyFinishedRecording = NO;
    startTime = kCMTimeInvalid;
    runSynchronouslyOnContextQueue(_movieWriterContext, ^{
        if(assetWriter.status == AVAssetWriterStatusFailed)
        {
            if(failureBlock){
                failureBlock([[NSError alloc] initWithDomain:@"GPUImageMovieWriter" code:-1 userInfo:nil]);
            }
            return;
        }
        if (audioInputReadyCallback == NULL)
        {
            [assetWriter startWriting];
        }
    });
    isRecording = YES;
	//    [assetWriter startSessionAtSourceTime:kCMTimeZero];
}

- (void)startRecordingInOrientation:(CGAffineTransform)orientationTransform;
{
	assetWriterVideoInput.transform = orientationTransform;

	[self startRecording];
}

- (void)cancelRecording;
{
    if (assetWriter.status == AVAssetWriterStatusCompleted)
    {
        return;
    }

    isRecording = NO;
    runSynchronouslyOnContextQueue(_movieWriterContext, ^{
        alreadyFinishedRecording = YES;

        if( assetWriter.status == AVAssetWriterStatusWriting && ! videoEncodingIsFinished )
        {
            videoEncodingIsFinished = YES;
            [assetWriterVideoInput markAsFinished];
        }
        if( assetWriter.status == AVAssetWriterStatusWriting && ! audioEncodingIsFinished )
        {
            audioEncodingIsFinished = YES;
            [assetWriterAudioInput markAsFinished];
        }
        [assetWriter cancelWriting];
    });
}

- (void)finishRecording;
{
    [self finishRecordingWithCompletionHandler:NULL];
}

- (void)finishRecordingWithCompletionHandler:(void (^)(void))handler;
{
    runSynchronouslyOnContextQueue(_movieWriterContext, ^{
        isRecording = NO;

        if (assetWriter.status == AVAssetWriterStatusCompleted || assetWriter.status == AVAssetWriterStatusCancelled || assetWriter.status == AVAssetWriterStatusUnknown)
        {
            if (handler)
                runAsynchronouslyOnContextQueue(_movieWriterContext, handler);
            return;
        }
        if( assetWriter.status == AVAssetWriterStatusWriting && ! videoEncodingIsFinished )
        {
            videoEncodingIsFinished = YES;
            [assetWriterVideoInput markAsFinished];
        }
        if( assetWriter.status == AVAssetWriterStatusWriting && ! audioEncodingIsFinished )
        {
            audioEncodingIsFinished = YES;
            [assetWriterAudioInput markAsFinished];
        }
#if (!defined(__IPHONE_6_0) || (__IPHONE_OS_VERSION_MAX_ALLOWED < __IPHONE_6_0))
        // Not iOS 6 SDK
        [assetWriter finishWriting];
        if (handler)
            runAsynchronouslyOnContextQueue(_movieWriterContext,handler);
#else
        // iOS 6 SDK
        if ([assetWriter respondsToSelector:@selector(finishWritingWithCompletionHandler:)]) {
            // Running iOS 6
            [assetWriter finishWritingWithCompletionHandler:(handler ?: ^{ })];
        }
        else {
            // Not running iOS 6
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
            [assetWriter finishWriting];
#pragma clang diagnostic pop
            if (handler)
                runAsynchronouslyOnContextQueue(_movieWriterContext, handler);
        }
#endif
    });
}

- (void)processVideoSampleBuffer:(CMSampleBufferRef)sampleBuffer
{
    // Only for @"DVGDirectAppend" = 1
    if (!isRecording || assetWriterPixelBufferInput != nil)
    {
        return;
    }

    CFRetain(sampleBuffer);

    CMTime currentSampleTime = CMSampleBufferGetOutputPresentationTimeStamp(sampleBuffer);

    if (CMTIME_IS_INVALID(startTime))
    {
        __weak GPUImageMovieWriter* wthis = self;
        runSynchronouslyOnContextQueue(_movieWriterContext, ^{
            GPUImageMovieWriter* sthis = wthis;
            if (CMTIME_IS_INVALID(sthis->startTime))
            {
                if(sthis.assetWriter.status == AVAssetWriterStatusFailed)
                {
                    sthis->isRecording = NO;
                    if(sthis.failureBlock){
                        sthis.failureBlock([[NSError alloc] initWithDomain:@"GPUImageMovieWriter" code:-1 userInfo:nil]);
                    }
                    return;
                }
                if ((sthis.videoInputReadyCallback == NULL) && (sthis.assetWriter.status != AVAssetWriterStatusWriting))
                {
                    [sthis.assetWriter startWriting];
                }

                [sthis.assetWriter startSessionAtSourceTime:currentSampleTime];
                sthis->startTime = currentSampleTime;
            }
        });
    }
    if (!isRecording)
    {
        CFRelease(sampleBuffer);
        return;
    }
    if (!assetWriterVideoInput.readyForMoreMediaData && _encodingLiveVideo)
    {
        NSLog(@"1: Had to drop an video frame: %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
        CFRelease(sampleBuffer);
        return;
    }
    if(isRecording){
        __weak GPUImageMovieWriter* wthis = self;
        void(^write)() = ^() {
            GPUImageMovieWriter* sthis = wthis;
            while( !sthis->assetWriterVideoInput.readyForMoreMediaData && !sthis->_encodingLiveVideo ) {
                NSDate *maxDate = [NSDate dateWithTimeIntervalSinceNow:0.5];
                [[NSRunLoop currentRunLoop] runUntilDate:maxDate];
            }
            if (!sthis->assetWriterVideoInput.readyForMoreMediaData)
            {
                NSLog(@"2: Had to drop an audio frame %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
            }
            else if(sthis.assetWriter.status == AVAssetWriterStatusWriting)
            {
				@try{
                    if (![sthis->assetWriterVideoInput appendSampleBuffer:sampleBuffer]){
                        NSLog(@"Problem appending video buffer at time: %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
                    }
                } @catch (NSException* exception) {
                    NSLog(@"3: Got exception: %@, Reason: %@", exception.name, exception.reason);
                    sthis->isRecording = NO;
                    if(sthis.failureBlock){
                        sthis.failureBlock([[NSError alloc] initWithDomain:@"GPUImageMovieWriter" code:-3 userInfo:nil]);
                    }
                }
            }
            else
            {
                //NSLog(@"Wrote an audio frame %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
            }
            CFRelease(sampleBuffer);
        };
        if( _encodingLiveVideo )
        {
            runAsynchronouslyOnContextQueue(_movieWriterContext, write);
        }
        else
        {
            write();
        }
    }
}

- (void)processAudioBuffer:(CMSampleBufferRef)audioBuffer;
{
    if (!isRecording)
    {
        return;
    }

//    if (_hasAudioTrack && CMTIME_IS_VALID(startTime))
    if (_hasAudioTrack)
    {
        CFRetain(audioBuffer);

        CMTime currentSampleTime = CMSampleBufferGetOutputPresentationTimeStamp(audioBuffer);
        __weak GPUImageMovieWriter* wthis = self;
        if (CMTIME_IS_INVALID(startTime))
        {
            runSynchronouslyOnContextQueue(_movieWriterContext, ^{
                GPUImageMovieWriter* sthis = wthis;
                if (CMTIME_IS_INVALID(sthis->startTime))
                {
                    if ((sthis->audioInputReadyCallback == NULL) && (sthis.assetWriter.status != AVAssetWriterStatusWriting))
                    {
                        if(sthis.assetWriter.status == AVAssetWriterStatusFailed)
                        {
                            sthis->isRecording = NO;
                            if(sthis.failureBlock){
                                sthis.failureBlock([[NSError alloc] initWithDomain:@"GPUImageMovieWriter" code:-1 userInfo:nil]);
                            }
                            return;
                        }
                        [sthis.assetWriter startWriting];
                    }
                    [sthis.assetWriter startSessionAtSourceTime:currentSampleTime];
                    sthis->startTime = currentSampleTime;
                }
            });
        }
        if (!isRecording)
        {
            CFRelease(audioBuffer);
            return;
        }
        if (!assetWriterAudioInput.readyForMoreMediaData && _encodingLiveVideo)
        {
            NSLog(@"1: Had to drop an audio frame: %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
            if (_shouldInvalidateAudioSampleWhenDone)
            {
                CMSampleBufferInvalidate(audioBuffer);
            }
            CFRelease(audioBuffer);
            return;
        }

        previousAudioTime = currentSampleTime;

        //if the consumer wants to do something with the audio samples before writing, let him.
        if (self.audioProcessingCallback) {
            //need to introspect into the opaque CMBlockBuffer structure to find its raw sample buffers.
            CMBlockBufferRef buffer = CMSampleBufferGetDataBuffer(audioBuffer);
            CMItemCount numSamplesInBuffer = CMSampleBufferGetNumSamples(audioBuffer);
            AudioBufferList audioBufferList;

            CMSampleBufferGetAudioBufferListWithRetainedBlockBuffer(audioBuffer,
                                                                    NULL,
                                                                    &audioBufferList,
                                                                    sizeof(audioBufferList),
                                                                    NULL,
                                                                    NULL,
                                                                    kCMSampleBufferFlag_AudioBufferList_Assure16ByteAlignment,
                                                                    &buffer
                                                                    );
            //passing a live pointer to the audio buffers, try to process them in-place or we might have syncing issues.
            for (int bufferCount=0; bufferCount < audioBufferList.mNumberBuffers; bufferCount++) {
                SInt16 *samples = (SInt16 *)audioBufferList.mBuffers[bufferCount].mData;
                self.audioProcessingCallback(&samples, numSamplesInBuffer);
            }
        }

        if(isRecording){
            __weak GPUImageMovieWriter* wthis = self;
            void(^write)() = ^() {
                GPUImageMovieWriter* sthis = wthis;
                while( !sthis->assetWriterAudioInput.readyForMoreMediaData && ! sthis->_encodingLiveVideo && ! sthis->audioEncodingIsFinished ) {
                    NSDate *maxDate = [NSDate dateWithTimeIntervalSinceNow:0.5];
                    //NSLog(@"audio waiting...");
                    [[NSRunLoop currentRunLoop] runUntilDate:maxDate];
                }
                if (!sthis->assetWriterAudioInput.readyForMoreMediaData)
                {
                    NSLog(@"2: Had to drop an audio frame %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
                }
                else if(sthis.assetWriter.status == AVAssetWriterStatusWriting)
                {
					@try{
                        if (![sthis->assetWriterAudioInput appendSampleBuffer:audioBuffer]){
                            NSLog(@"Problem appending audio buffer at time: %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
                        }
                    } @catch (NSException* exception) {
                        NSLog(@"3: Got exception: %@, Reason: %@", exception.name, exception.reason);
                        sthis->isRecording = NO;
                        if(sthis.failureBlock){
                            sthis.failureBlock([[NSError alloc] initWithDomain:@"GPUImageMovieWriter" code:-2 userInfo:nil]);
                        }
                    }
                }
                else
                {
                    //NSLog(@"Wrote an audio frame %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, currentSampleTime)));
                }

                if (sthis->_shouldInvalidateAudioSampleWhenDone)
                {
                    CMSampleBufferInvalidate(audioBuffer);
                }
                CFRelease(audioBuffer);
            };
    //        runAsynchronouslyOnContextQueue(_movieWriterContext, write);
            if(_encodingLiveVideo )

            {
                runAsynchronouslyOnContextQueue(_movieWriterContext, write);
            }
            else
            {
                write();
            }
        }
    }
}

- (void)enableSynchronizationCallbacks;
{
    if (videoInputReadyCallback != NULL)
    {
        __weak GPUImageMovieWriter* wthis = self;
        runSynchronouslyOnContextQueue(_movieWriterContext, ^{
            GPUImageMovieWriter* sthis = wthis;
            if( sthis.assetWriter.status != AVAssetWriterStatusWriting && sthis.assetWriter.status <= AVAssetWriterStatusCompleted)
            {
                if(sthis.assetWriter.status == AVAssetWriterStatusFailed)
                {
                    sthis->isRecording = NO;
                    if(sthis.failureBlock){
                        sthis.failureBlock([[NSError alloc] initWithDomain:@"GPUImageMovieWriter" code:-1 userInfo:nil]);
                    }
                    return;
                }
                [sthis.assetWriter startWriting];
            }
        });
        videoQueue = dispatch_queue_create("com.sunsetlakesoftware.GPUImage.videoReadingQueue", DISPATCH_QUEUE_SERIAL);
        [assetWriterVideoInput requestMediaDataWhenReadyOnQueue:videoQueue usingBlock:^{
            if( _paused )
            {
                //NSLog(@"video requestMediaDataWhenReadyOnQueue paused");
                // if we don't sleep, we'll get called back almost immediately, chewing up CPU
                usleep(10000);
                return;
            }
            //NSLog(@"video requestMediaDataWhenReadyOnQueue begin");
            while( assetWriterVideoInput.readyForMoreMediaData && ! _paused )
            {
                if( videoInputReadyCallback && ! videoInputReadyCallback() && ! videoEncodingIsFinished )
                {
                    runAsynchronouslyOnContextQueue(_movieWriterContext, ^{
                        if( assetWriter.status == AVAssetWriterStatusWriting && ! videoEncodingIsFinished )
                        {
                            videoEncodingIsFinished = YES;
                            [assetWriterVideoInput markAsFinished];
                        }
                    });
                }
            }
            //NSLog(@"video requestMediaDataWhenReadyOnQueue end");
        }];
    }

    if (audioInputReadyCallback != NULL)
    {
        audioQueue = dispatch_queue_create("com.sunsetlakesoftware.GPUImage.audioReadingQueue", DISPATCH_QUEUE_SERIAL);
        [assetWriterAudioInput requestMediaDataWhenReadyOnQueue:audioQueue usingBlock:^{
            if( _paused )
            {
                //NSLog(@"audio requestMediaDataWhenReadyOnQueue paused");
                // if we don't sleep, we'll get called back almost immediately, chewing up CPU
                usleep(10000);
                return;
            }
            //NSLog(@"audio requestMediaDataWhenReadyOnQueue begin");
            while( assetWriterAudioInput.readyForMoreMediaData && ! _paused )
            {
                if( audioInputReadyCallback && ! audioInputReadyCallback() && ! audioEncodingIsFinished )
                {
                    runAsynchronouslyOnContextQueue(_movieWriterContext, ^{
                        if( assetWriter.status == AVAssetWriterStatusWriting && ! audioEncodingIsFinished )
                        {
                            audioEncodingIsFinished = YES;
                            [assetWriterAudioInput markAsFinished];
                        }
                    });
                }
            }
            //NSLog(@"audio requestMediaDataWhenReadyOnQueue end");
        }];
    }

}

#pragma mark -
#pragma mark Frame rendering

- (void)createDataFBO;
{
    [self prepareDataFBO];
}

- (CVPixelBufferRef)prepareDataFBO; // TODO: khm... name doesn't correpond well with the return type
{
    glActiveTexture(GL_TEXTURE1);
    glGenFramebuffers(1, &movieFramebuffer);
    glBindFramebuffer(GL_FRAMEBUFFER, movieFramebuffer);

    CVPixelBufferPoolCreatePixelBuffer (NULL, [assetWriterPixelBufferInput pixelBufferPool], &renderTarget);
    CVOpenGLESTextureCacheCreateTextureFromImage (kCFAllocatorDefault, [_movieWriterContext coreVideoTextureCache], renderTarget,
                                                  NULL, // texture attributes
                                                  GL_TEXTURE_2D,
                                                  GL_RGBA, // opengl format
                                                  (int)videoSize.width,
                                                  (int)videoSize.height,
                                                  GL_BGRA, // native iOS format
                                                  GL_UNSIGNED_BYTE,
                                                  0,
                                                  &renderTexture);

    glBindTexture(CVOpenGLESTextureGetTarget(renderTexture), CVOpenGLESTextureGetName(renderTexture));
    glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_CLAMP_TO_EDGE);
    glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_CLAMP_TO_EDGE);

    glFramebufferTexture2D(GL_FRAMEBUFFER, GL_COLOR_ATTACHMENT0, GL_TEXTURE_2D, CVOpenGLESTextureGetName(renderTexture), 0);

    GLenum status = glCheckFramebufferStatus(GL_FRAMEBUFFER);
    NSAssert(status == GL_FRAMEBUFFER_COMPLETE, @"Incomplete filter FBO: %d", status);

    return renderTarget;
}

- (void)getRidOfDataFBO;
{
    if (renderTexture) {
        CFRelease(renderTexture);
        renderTexture = NULL;
    }
    if (renderTarget) {
        CVPixelBufferRelease(renderTarget);
        renderTarget = NULL;
    }
    if (movieFramebuffer) {
        glDeleteFramebuffers(1, &movieFramebuffer);
        movieFramebuffer = 0;
    }
}

- (void)destroyDataFBO;
{
    runSynchronouslyOnContextQueue(_movieWriterContext, ^{
        [_movieWriterContext useAsCurrentContext];
        [self getRidOfDataFBO];
    });
}

- (void)setFilterFBO;
{
    if (!movieFramebuffer)
    {
        [self createDataFBO];
    }

    glBindFramebuffer(GL_FRAMEBUFFER, movieFramebuffer);

    glViewport(0, 0, (int)videoSize.width, (int)videoSize.height);
}

- (void)renderAtInternalSizeUsingFramebuffer:(GPUImageFramebuffer *)inputFramebufferToUse;
{
    [_movieWriterContext useAsCurrentContext];
    [self setFilterFBO];

    [_movieWriterContext setContextShaderProgram:colorSwizzlingProgram];

    glClearColor(1.0f, 0.0f, 0.0f, 1.0f);
    glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT);

    // This needs to be flipped to write out to video correctly
    static const GLfloat squareVertices[] = {
        -1.0f, -1.0f,
        1.0f, -1.0f,
        -1.0f,  1.0f,
        1.0f,  1.0f,
    };

    const GLfloat *textureCoordinates = [GPUImageFilter textureCoordinatesForRotation:inputRotation];

	glActiveTexture(GL_TEXTURE4);
	glBindTexture(GL_TEXTURE_2D, [inputFramebufferToUse texture]);
	glUniform1i(colorSwizzlingInputTextureUniform, 4);

//    NSLog(@"Movie writer framebuffer: %@", inputFramebufferToUse);

    glVertexAttribPointer(colorSwizzlingPositionAttribute, 2, GL_FLOAT, 0, 0, squareVertices);
	glVertexAttribPointer(colorSwizzlingTextureCoordinateAttribute, 2, GL_FLOAT, 0, 0, textureCoordinates);

    glDrawArrays(GL_TRIANGLE_STRIP, 0, 4);
    glFinish();
}

#pragma mark -
#pragma mark GPUImageInput protocol

- (void)newFrameReadyAtTime:(CMTime)frameTime atIndex:(NSInteger)textureIndex;
{
    if (!isRecording)
    {
        [firstInputFramebuffer unlock];
        return;
    }

    // Drop frames forced by images and other things with no time constants
    // Also, if two consecutive times with the same value are added to the movie, it aborts recording, so I bail on that case
    if ( (CMTIME_IS_INVALID(frameTime)) || (CMTIME_COMPARE_INLINE(frameTime, ==, previousFrameTime)) || (CMTIME_IS_INDEFINITE(frameTime)) )
    {
        [firstInputFramebuffer unlock];
        return;
    }

    if (CMTIME_IS_INVALID(startTime))
    {
        __weak GPUImageMovieWriter* wthis = self;
        runSynchronouslyOnContextQueue(_movieWriterContext, ^{
            GPUImageMovieWriter* sthis = wthis;
            if (CMTIME_IS_INVALID(sthis->startTime))
            {
                if(sthis.assetWriter.status == AVAssetWriterStatusFailed)
                {
                    sthis->isRecording = NO;
                    if(sthis.failureBlock){
                        sthis.failureBlock([[NSError alloc] initWithDomain:@"GPUImageMovieWriter" code:-1 userInfo:nil]);
                    }
                    return;
                }
                if ((sthis.videoInputReadyCallback == NULL) && (sthis.assetWriter.status != AVAssetWriterStatusWriting))
                {
                    [sthis.assetWriter startWriting];
                }

                [sthis.assetWriter startSessionAtSourceTime:frameTime];
                sthis->startTime = frameTime;
            }
        });
    }
    if (!isRecording)
    {
        [firstInputFramebuffer unlock];
        return;
    }
    GPUImageFramebuffer *inputFramebufferForBlock = firstInputFramebuffer;
    glFinish();

    runSynchronouslyOnContextQueue(_movieWriterContext, ^{
        // TODO: make sure writer is ready for input = our renderTarget is free to be modified
        if (!assetWriterVideoInput.readyForMoreMediaData && _encodingLiveVideo)
        {
            [inputFramebufferForBlock unlock];
            NSLog(@"1: Had to drop a video frame: %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, frameTime)));
            return;
        }

        while (!assetWriterVideoInput.readyForMoreMediaData && !_encodingLiveVideo && !videoEncodingIsFinished) {
            printf("....\n");
            NSDate *maxDate = [NSDate dateWithTimeIntervalSinceNow:0.1];
            [[NSRunLoop currentRunLoop] runUntilDate:maxDate];
        }
//        usleep((useconds_t)(0.1 * 1.0e6));

        // Render the frame with swizzled colors, so that they can be uploaded quickly as BGRA frames
//        [_movieWriterContext useAsCurrentContext]; // set inside renderAtInternalSize...:
        CVPixelBufferRef pixelBuffer = [self prepareDataFBO];
        [self renderAtInternalSizeUsingFramebuffer:inputFramebufferForBlock];

        if(self.assetWriter.status == AVAssetWriterStatusWriting) {
            if (![assetWriterPixelBufferInput appendPixelBuffer:pixelBuffer withPresentationTime:frameTime])
                NSLog(@"Problem appending pixel buffer at time: %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, frameTime)));
        }
        else {
            NSLog(@"Couldn't write a frame");
            //NSLog(@"Wrote a video frame: %@", CFBridgingRelease(CMTimeCopyDescription(kCFAllocatorDefault, frameTime)));
        }
        [self getRidOfDataFBO];
//            CVPixelBufferUnlockBaseAddress(pixel_buffer, 0);

        previousFrameTime = frameTime;

//            if (![GPUImageContext supportsFastTextureUpload]) {
//                CVPixelBufferRelease(pixel_buffer);
//            }
//        [self destroyDataFBO];

        [inputFramebufferForBlock unlock];
    });
}

- (NSInteger)nextAvailableTextureIndex;
{
    return 0;
}

- (void)setInputFramebuffer:(GPUImageFramebuffer *)newInputFramebuffer atIndex:(NSInteger)textureIndex;
{
    [newInputFramebuffer lock];
//    runSynchronouslyOnContextQueue(_movieWriterContext, ^{
        firstInputFramebuffer = newInputFramebuffer;
//    });
}

- (void)setInputRotation:(GPUImageRotationMode)newInputRotation atIndex:(NSInteger)textureIndex;
{
    inputRotation = newInputRotation;
}

- (void)setInputSize:(CGSize)newSize atIndex:(NSInteger)textureIndex;
{
}

- (CGSize)maximumOutputSize;
{
    return videoSize;
}

- (void)endProcessing
{
    if (completionBlock)
    {
        if (!alreadyFinishedRecording)
        {
            alreadyFinishedRecording = YES;
            completionBlock();
        }
    }
    else
    {
        if (_delegate && [_delegate respondsToSelector:@selector(movieRecordingCompleted)])
        {
            [_delegate movieRecordingCompleted];
        }
    }
}

- (BOOL)shouldIgnoreUpdatesToThisTarget;
{
    return NO;
}

- (BOOL)wantsMonochromeInput;
{
    return NO;
}

- (void)setCurrentlyReceivingMonochromeInput:(BOOL)newValue;
{

}

#pragma mark -
#pragma mark Accessors

- (void)setHasAudioTrack:(BOOL)newValue
{
	[self setHasAudioTrack:newValue audioSettings:nil];
}

- (void)setHasAudioTrack:(BOOL)newValue audioSettings:(NSDictionary *)audioOutputSettings;
{
    _hasAudioTrack = newValue;

    if (_hasAudioTrack)
    {
        if (_shouldPassthroughAudio)
        {
			// Do not set any settings so audio will be the same as passthrough
			audioOutputSettings = nil;
        }
        else if (audioOutputSettings == nil)
        {
            AVAudioSession *sharedAudioSession = [AVAudioSession sharedInstance];
            double preferredHardwareSampleRate;

            if ([sharedAudioSession respondsToSelector:@selector(sampleRate)])
            {
                preferredHardwareSampleRate = [sharedAudioSession sampleRate];
            }
            else
            {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
                preferredHardwareSampleRate = [[AVAudioSession sharedInstance] currentHardwareSampleRate];
#pragma clang diagnostic pop
            }

            AudioChannelLayout acl;
            bzero( &acl, sizeof(acl));
            acl.mChannelLayoutTag = kAudioChannelLayoutTag_Mono;

            audioOutputSettings = [NSDictionary dictionaryWithObjectsAndKeys:
                                         [ NSNumber numberWithInt: kAudioFormatMPEG4AAC], AVFormatIDKey,
                                         [ NSNumber numberWithInt: 1 ], AVNumberOfChannelsKey,
                                         [ NSNumber numberWithFloat: preferredHardwareSampleRate ], AVSampleRateKey,
                                         [ NSData dataWithBytes: &acl length: sizeof( acl ) ], AVChannelLayoutKey,
                                         //[ NSNumber numberWithInt:AVAudioQualityLow], AVEncoderAudioQualityKey,
                                         [ NSNumber numberWithInt: 64000 ], AVEncoderBitRateKey,
                                         nil];
/*
            AudioChannelLayout acl;
            bzero( &acl, sizeof(acl));
            acl.mChannelLayoutTag = kAudioChannelLayoutTag_Mono;

            audioOutputSettings = [NSDictionary dictionaryWithObjectsAndKeys:
                                   [ NSNumber numberWithInt: kAudioFormatMPEG4AAC ], AVFormatIDKey,
                                   [ NSNumber numberWithInt: 1 ], AVNumberOfChannelsKey,
                                   [ NSNumber numberWithFloat: 44100.0 ], AVSampleRateKey,
                                   [ NSNumber numberWithInt: 64000 ], AVEncoderBitRateKey,
                                   [ NSData dataWithBytes: &acl length: sizeof( acl ) ], AVChannelLayoutKey,
                                   nil];*/
        }

        assetWriterAudioInput = [AVAssetWriterInput assetWriterInputWithMediaType:AVMediaTypeAudio outputSettings:audioOutputSettings];
        [assetWriter addInput:assetWriterAudioInput];
        assetWriterAudioInput.expectsMediaDataInRealTime = _encodingLiveVideo;
    }
    else
    {
        // Remove audio track if it exists
    }
}

- (NSArray*)metaData {
    return assetWriter.metadata;
}

- (void)setMetaData:(NSArray*)metaData {
    assetWriter.metadata = metaData;
}

- (CMTime)duration {
    if( ! CMTIME_IS_VALID(startTime) )
        return kCMTimeZero;
    if( ! CMTIME_IS_NEGATIVE_INFINITY(previousFrameTime) )
        return CMTimeSubtract(previousFrameTime, startTime);
    if( ! CMTIME_IS_NEGATIVE_INFINITY(previousAudioTime) )
        return CMTimeSubtract(previousAudioTime, startTime);
    return kCMTimeZero;
}

- (CGAffineTransform)transform {
    return assetWriterVideoInput.transform;
}

- (void)setTransform:(CGAffineTransform)transform {
    assetWriterVideoInput.transform = transform;
}

- (AVAssetWriter*)assetWriter {
    return assetWriter;
}

@end
