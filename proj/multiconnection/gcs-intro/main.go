package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:          "gcsintro",
	SilenceUsage: true,
}

var uploadObj = &cobra.Command{
	Use:  "obj",
	RunE: runUploadObj,
}

var uploadObjArgs struct {
	bucket string
}

func init() {
	uploadObj.Flags().StringVarP(&uploadObjArgs.bucket, "bucket", "b", "", "destination bucket")
	rootCmd.AddCommand(uploadObj)
}

func runUploadObj(self *cobra.Command, args []string) (err error) {
	ctx := context.Background()

	if uploadObjArgs.bucket == "" {
		return errors.New("destination bucket must be specified")
	}

	c, err := NewGcsClient(ctx)
	if err != nil {
		return err
	}

	const runs = 16
	const minSize int = 1 << 20
	const maxSize int = 128 << 20

	for size := minSize; size <= maxSize; size <<= 1 {
		buf := makeRandBuf(size)
		durations := make([]float64, 0, runs)

		for i := range runs {
			d, err := c.UploadObject(ctx, uploadObjArgs.bucket, fmt.Sprintf("x-%d-%d", size/minSize, i), buf)
			if err != nil {
				return err
			}
			durations = append(durations, d)
		}

		var meanTime float64
		for _, d := range durations {
			meanTime += d
		}
		meanTime = meanTime / float64(len(durations))
		meanSpeed := float64(size/minSize) / meanTime
		var sigma float64
		for _, d := range durations {
			diff := meanSpeed - float64(size/minSize)/d
			sigma += diff * diff
		}
		sigma = math.Sqrt(sigma / float64(len(durations)))

		fmt.Printf("size=%3d MiB: mean speed=%.3f MiB/s, standard deviation=%.6f MiB/s\n",
			size/minSize, meanSpeed, sigma)
	}

	return nil
}

var uploadMultipartObj = &cobra.Command{
	Use:  "mobj",
	RunE: runUploadMultipartObj,
}

var uploadMultipartObjArgs struct {
	bucket string
}

func init() {
	uploadMultipartObj.Flags().StringVarP(&uploadMultipartObjArgs.bucket, "bucket", "b", "", "destination bucket")
	rootCmd.AddCommand(uploadMultipartObj)
}

func runUploadMultipartObj(self *cobra.Command, args []string) (err error) {
	ctx := context.Background()

	if uploadMultipartObjArgs.bucket == "" {
		return errors.New("destination bucket must be specified")
	}

	c, err := NewGcsClient(ctx)
	if err != nil {
		return err
	}

	const runs int = 16
	const minSize int = 1 << 20   // 1 MiB
	const maxSize int = 128 << 20 // 128 MiB

	for size := minSize; size <= maxSize; size <<= 1 {

		uploadUrl, err := c.NewUploadSession(ctx, uploadMultipartObjArgs.bucket, fmt.Sprintf("x-%d", size/minSize))
		if err != nil {
			return err
		}

		off, buf := int64(0), makeRandBuf(size)

		durations := make([]float64, 0, runs)

		for i := range runs {
			d, err := c.UploadObjectPart(ctx, uploadUrl, off, buf, i == runs-1)
			if err != nil {
				return err
			}
			durations = append(durations, d)

			off += int64(size)

			testOff, testLast, err := c.GetResumeOffset(ctx, uploadUrl)
			if err != nil {
				return err
			}
			if testOff != off {
				return fmt.Errorf("unexpected offset: got %d, want %d", testOff, off)
			}
			if testLast != (i == runs-1) {
				return fmt.Errorf("unexpected last part flag: got %t, want %t", testLast, i == runs-1)
			}
		}
		var meanTime float64
		for _, d := range durations {
			meanTime += d
		}
		meanTime = meanTime / float64(len(durations))
		meanSpeed := float64(size/minSize) / meanTime
		var sigma float64
		for _, d := range durations {
			diff := meanSpeed - float64(size/minSize)/d
			sigma += diff * diff
		}
		sigma = math.Sqrt(sigma / float64(len(durations)))

		fmt.Printf("size=%3d MiB: mean speed=%.3f MiB/s, standard deviation=%.6f MiB/s\n",
			size/minSize, meanSpeed, sigma)

	}
	return nil
}
