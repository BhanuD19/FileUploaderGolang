package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var awsS3Client *s3.Client
var wg = new(sync.WaitGroup)
var (
	BillApi                       string
	S3BucketBill                  string
	S3BucketLedger                string
	LedgerPath                    string
	BillPath                      string
	FilesPath                     string
	LedgerApi, authToken, authApi string
)

var done chan bool

func main() {
	fmt.Println("Program started!")
	go ConfigStart(wg)
	wg.Add(1)
	maxQ, err := strconv.Atoi(os.Getenv("max.queue.length"))
	if err != nil {
		log.Printf("\nCouldn't read max queue length value. Setting to 100")
		maxQ = 100
	}
	maxW, err := strconv.Atoi(os.Getenv("max.workers.number"))
	if err != nil {
		log.Printf("\nCouldn't read max workers number value. Setting to 5")
		maxW = 10
	}
	done = make(chan bool)
	wg.Wait()

	go GetAuthToken("5678998765", wg)
	wg.Add(1)

	var files = sync.Map{}
	fmt.Printf("FilePaths from env: %s", FilesPath)
	filePaths := strings.Split(FilesPath, ",")
	fmt.Printf("\nFilePaths after splitting with ',': %v, fileType: %T", filePaths, filePaths)
	for _, v := range filePaths {
		go func(folderName string, wg *sync.WaitGroup) {
			keyName := strings.Split(folderName, "-")
			s := keyName[0][2:]
			fileList := readFiles(folderName)
			fmt.Printf("For key %s got the value from readfiles %v", s, fileList)
			files.Store(s, fileList)
			wg.Done()
		}(v, wg)
		wg.Add(1)
	}
	wg.Wait()

	//fmt.Printf("Got the files %s and starting job", files)
	//fmt.Printf("\nDispatcher property %v", Disp)
	jobQ := make(chan Job, maxQ)
	for i := 0; i < maxW; i++ {
		go func(i int) {
			for j := range jobQ {
				log.Printf("\n Got the job in queue, %v", j)
				StartJobs(i, j)
			}
		}(i)
	}

	files.Range(func(key, value interface{}) bool {
		files2 := value.([]string)
		if key == nil {
			return false
		}
		log.Printf("\nUploading file %s,from %v", key, files2)
		for _, s := range files2 {
			fmt.Printf("\n Uploading file : %s", s)
			job := Job{
				fileName: s,
				fileType: key.(string),
			}
			fmt.Printf("\n Appending to the JobQ %v", job)
			go func() { jobQ <- job }()
		}
		return true
	})

	<-done
	defer func() {
		close(jobQ)
		close(done)
	}()
}

func ConfigStart(waitGr *sync.WaitGroup) {
	fmt.Println("\nProgram config started!")
	dir := "./"
	environmentPath := filepath.Join(dir, ".env")
	err := godotenv.Load(environmentPath)
	if err != nil {
		log.Fatal(err)
	}
	accessKey := os.Getenv("cloud.aws.credentials.accessKey")
	secretKey := os.Getenv("cloud.aws.credentials.secretKey")
	region := os.Getenv("cloud.aws.region.static")

	LedgerApi = os.Getenv("ledger.upload.api")
	BillApi = os.Getenv("billing.upload.api")
	S3BucketBill = os.Getenv("aws.bucket.billing.pdf")
	S3BucketLedger = os.Getenv("aws.bucket.ledger.pdf")
	FilesPath = os.Getenv("upload.file.paths")
	authApi = os.Getenv("authentication.bizzsetu.api")

	filesPathArr := strings.Split(FilesPath, ",")
	LedgerPath = filesPathArr[1]
	BillPath = filesPathArr[0]
	//must := session2.Must(session2.NewSessionWithOptions(session2.Options{
	//	SharedConfigState: session2.SharedConfigEnable,
	//}))
	//if err != nil {
	//	log.Fatalf("\nGot error while creating sessions : %v", err)
	//}
	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	credsTemp, err := creds.Retrieve(context.Background())
	if err != nil {
		log.Fatalf("\n Error while getting credentials: %v", err)
	} else {
		log.Printf("\n Got credentials : %v", credsTemp)
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithCredentialsProvider(creds), config.WithRegion(region))
	if err != nil {
		log.Fatalf("\nerror: %v", err)
		return
	}
	awsS3Client = s3.NewFromConfig(cfg)

	fmt.Println("\nProgram config ended!")
	waitGr.Done()
}

//func configS3() {
//	//log.Printf("Value of map %s", files)
//	//for k, file := range files {
//	//	fmt.Println("Uploading file:", file)
//	//	fName := file[:strings.LastIndex(file, ".")]
//	//	ext := file[(strings.Index(file, ".") + 1):]
//	//	if !strings.EqualFold(ext, "pdf") {
//	//		continue
//	//	}
//	//	fNameArr := strings.Split(fName, "~")
//	//	err := UploadFile(S3BucketBill, fNameArr[0]+"/"+time.Now().Format("2006-01-02")+"/"+strconv.Itoa(time.Now().UTC().Nanosecond())+"_"+fName, file)
//	//	if err != nil {
//	//		log.Fatal(err)
//	//	}
//	//}
//	//UploadFile(S3_BUCKET, "2519/161318882232_2519/Plugnplay.pdf", "./ledger-pdf/Plugnplay.pdf")
//}

// readFiles reads files in the path passed to the function. returns a slice of filenames
func readFiles(path string) []string {
	fmt.Printf("\nreading files started! from: %s", path)
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("\nopening file error for : ", err)
		return nil
	}
	var fList []string
	files, err := f.Readdir(0)
	if err != nil {
		fmt.Println("\nReaddir error for : ", err)
		return nil
	}

	for _, v := range files {
		if !v.IsDir() {
			fileName := v.Name()
			fmt.Printf("Currently reading file : %s", fileName)
			if len(fileName) > 0 {
				ext := fileName[(strings.LastIndex(fileName, ".") + 1):]
				fmt.Printf("extension: %s", ext)
				if ext != "pdf" {
					continue
				}
				fList = append(fList, v.Name())
			}
		}
	}
	return fList
}

//func upload() {
//	upFile, err := os.Open("./ledger-pdf/Plugnplay.pdf")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer upFile.Close()
//	upfileInfo, _ := upFile.Stat()
//	fmt.Println("File stats : ", upfileInfo)
//	var fileSize int64 = upfileInfo.Size()
//	fileBuffer := make([]byte, fileSize)
//	upFile.Read(fileBuffer)
//
//	session, err :=
//	if err != nil {
//		log.Fatal("session.NewSession err:", err)
//	}
//}

// UploadFile reads from a file and puts the data into an object in a bucket.
func UploadFile(objectKey string, fileName string, fileType string) error {
	fmt.Printf("\nUploading to AWS started with:  %s, %s, %s", objectKey, fileName, fileType)
	dir := "./"
	var err error
	switch fileType {
	case "bills":
		fp := filepath.Join(dir, BillPath)
		fPath := filepath.Join(fp, fileName)
		file, err := os.Open(fPath)
		if err != nil {
			log.Printf("\nCouldn't open file %v to upload. Here's why: %v\n", fileName, err)
		} else {
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					log.Printf("\nCouldn't close file %v. Here's why: %v\n",
						fileName, err)
				}
			}(file)
			if awsS3Client == nil {
				log.Fatalf("\n Got nill configs for AWS client %v", awsS3Client)
			} else {
				log.Printf("\n Got non nill configs for AWS client %v", awsS3Client)
			}
			resp, err := awsS3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(S3BucketBill),
				Key:    aws.String(objectKey),
				Body:   file,
			})
			if err != nil {
				log.Printf("\nCouldn't upload file %v to %v:%v. Here's why: %v\n",
					fileName, S3BucketBill, objectKey, err)
			}
			UploadBillPdfToBizzsetu(fileName, "5678998765", objectKey, resp.VersionId)
		}

	case "ledger":
		fp := filepath.Join(dir, LedgerPath)
		fPath := filepath.Join(fp, fileName)
		file, err := os.Open(fPath)
		if err != nil {
			log.Printf("\nCouldn't open file %v to upload. Here's why: %v\n", fileName, err)
		} else {
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					log.Printf("\nCouldn't close file %v. Here's why: %v\n",
						fileName, err)
				}
			}(file)
			if awsS3Client == nil {
				log.Fatalf("\n Got nill configs for AWS client ledger : %v", awsS3Client)
			} else {
				log.Printf("\n Got non nill configs for AWS client ledger : %v", awsS3Client)
			}
			resp, err := awsS3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(S3BucketLedger),
				Key:    aws.String(objectKey),
				Body:   file,
			})
			if err != nil {
				log.Printf("\nCouldn't upload file %v to %v:%v. Here's why: %v\n",
					fileName, S3BucketLedger, objectKey, err)
			}
			UploadLedgerPdfToBizzsetu(fileName, "5678998765", objectKey, resp.VersionId)
		}
	}
	return err
}

func UploadBillPdfToBizzsetu(fileName string, mobNo string, key string, id *string) {
	client := &http.Client{
		Timeout: time.Second * 10,
	}
	log.Printf("\nUpload to bizzsetu Bills fileName: %s, key: %s, version: %s", fileName, key, *id)
	if authToken == "" {
		authToken = GetAuthToken(mobNo, nil)
	}
	fNameArr := strings.Split(fileName, "~")
	upload := map[string]string{
		"sellerEnId": strings.Trim(fNameArr[0], " "), "billSourceType": "pdf", "source": "tally", "buyerBillToBuyerName": strings.Trim(fNameArr[1], " "), "billNo": strings.Trim(fNameArr[2], " "), "billIssueDate": fNameArr[3], "totalAmount": fNameArr[4], "billPdfKey": strings.Trim(key, " "), "billPdfVersionId": strings.Trim(*id, " "), "mobNo": mobNo,
	}
	reqBody, _ := json.Marshal(upload)
	req, err := http.NewRequest("POST", BillApi, bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatalf("\nError while making request : %s", err)
	}
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Content-Type", "application/json")
	log.Printf("\nRequest formed %s, %s, %s", req.Header, req.URL, req.Body)
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
	resBody, _ := io.ReadAll(res.Body)
	log.Printf("\nResponse body: %s", resBody)
	respStr := string(resBody)
	resp := []byte(respStr)
	log.Printf("\nResponse from server: %s", resp)

	var jsonRes map[string]interface{}
	_ = json.Unmarshal(resp, &jsonRes)
	log.Printf("\nResponse after %T", jsonRes)
	for k, v := range jsonRes {
		log.Printf("\nvalue and key for response from bill : %s, %s", v, k)
	}
	go DeleteFile(fileName, "bills")
}

func UploadLedgerPdfToBizzsetu(fileName string, mobNo string, key string, id *string) {
	client := &http.Client{
		Timeout: time.Second * 10,
	}
	log.Printf("\nUpload to bizzsetu Ledger fileName: %s, key: %s, version: %s", fileName, key, *id)
	if authToken == "" {
		authToken = GetAuthToken(mobNo, nil)
	}
	fNameArr := strings.Split(fileName, "~")
	fName := fNameArr[1]
	fName = strings.Trim(fName, " ")
	fName = fName[0:strings.LastIndex(fNameArr[1], ".")]
	upload := map[string]string{
		"sellerEnId": fNameArr[0], "ledgerToBuyerName": fName, "pdfKey": key, "versionId": *id, "mobNo": mobNo,
	}
	reqBody, _ := json.Marshal(upload)
	req, err := http.NewRequest("POST", LedgerApi, bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatalf("Error while making request : %s", err)
	}
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Content-Type", "application/json")
	log.Printf("\nRequest formed %s, %s, %s", req.Header, req.URL, req.Body)
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
	resBody, _ := io.ReadAll(res.Body)
	log.Printf("\nResponse body: %s", resBody)
	respStr := string(resBody)
	resp := []byte(respStr)
	log.Printf("\nResponse from server: %s", resp)

	var jsonRes map[string]interface{}
	_ = json.Unmarshal(resp, &jsonRes)
	log.Printf("\nResponse after %T", jsonRes)
	for k, v := range jsonRes {
		log.Printf("\nvalue and key response from ledger: %s, %s", v, k)
	}
	go DeleteFile(fileName, "ledger")
}

func DeleteFile(fileName, fileType string) {
	if fileName != "" {
		dir := "./"
		var fp string
		switch fileType {
		case "bills":
			fp = filepath.Join(dir, BillPath)
		case "ledger":
			fp = filepath.Join(dir, LedgerPath)
		}
		if fp != "" {
			fPath := filepath.Join(fp, fileName)
			if fPath != "" {
				err := os.Remove(fPath)
				if err != nil {
					log.Printf("\nError while removing files")
				}
			}
		}
	}
}

func GetAuthToken(mobNo string, waitGr *sync.WaitGroup) string {
	log.Printf("\nGetting auth token with mobNo: %s", mobNo)
	authReq := map[string]string{
		"username": mobNo, "password": mobNo,
	}
	reqBody, _ := json.Marshal(authReq)
	response, err := http.Post(authApi, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatal(err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(response.Body)

	resBody, _ := io.ReadAll(response.Body)
	respStr := string(resBody)
	resp := []byte(respStr)
	log.Printf("\nResponse from server: %s", resp)
	var jsonRes map[string]string
	_ = json.Unmarshal(resp, &jsonRes)
	authToken = jsonRes["jwtToken"]
	waitGr.Done()
	return authToken
}

type Job struct {
	fileName, fileType string
}

func StartJobs(i int, j Job) {
	fmt.Printf("\nStarting Job with ID %d", i)
	fmt.Printf("\nGot job %v", j)
	fNameArr := strings.Split(j.fileName, "~")
	err := UploadFile(fNameArr[0]+"/"+time.Now().Format("2006-01-02")+"/"+strconv.Itoa(time.Now().UTC().Nanosecond())+"_"+j.fileName, j.fileName, j.fileType)
	if err != nil {
		log.Fatal(err)
	}

}
