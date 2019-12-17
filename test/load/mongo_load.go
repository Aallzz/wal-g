package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tinsane/tracelog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"strings"
	"sync"
)

var contextByName = map[string]context.Context{
	"todo":       context.TODO(),
	"background": context.Background(),
}

type MongoWorker struct {
	wg sync.WaitGroup
	ch chan interface{}
	fs []func()
}

func CreateMongoWorker(chSz int) MongoWorker {
	return MongoWorker{ch: make(chan interface{}, chSz)}
}

func getStringType(data interface{}) string {
	v := reflect.ValueOf(data)
	switch v.Kind() {
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.String:
		return "string"
	case reflect.Slice:
		return "array"
	case reflect.Map:
		return "map"
	case reflect.Chan:
		return "chan"
	default:
		return "unknown"
	}
}

func (mw *MongoWorker) addMongoOp(client *mongo.Client, sop string) error {
	var arr []interface{}
	var err error
	err = json.Unmarshal([]byte(sop), &arr)
	if err != nil {
		return err
	}
	for _, opdata := range arr {
		x, ok := opdata.(map[string]interface{})
		if !ok {
			return fmt.Errorf("command expected to be a map, but found %s", getStringType(opdata))
		}
		switch x["op"] {
		case "c":
			commandDoc, err := parseCommandOp(x)
			if err != nil {
				return err
			}
			mw.mongoRunCommandOp(client, commandDoc)
		default:
			return fmt.Errorf("unknown command %v", x["op"])
		}
	}
	return nil
}

type MongoRunCommandOp struct {
	DbName   string `json:"db"`
	Ctx      string `json:"ctx"`
	MakeLogs bool   `json:"makelogs"`
	Doc      bson.D
}

func parseCommandOp(cd map[string]interface{}) (*MongoRunCommandOp, error) {
	var res MongoRunCommandOp
	bmrco, err := json.Marshal(cd)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	err = json.Unmarshal(bmrco, &res)
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	x, err := bson.Marshal(cd["dc"])
	if err != nil {
		return nil, fmt.Errorf("cannot parse opts for commad: %+v", err)
	}
	var y bson.D
	err = bson.Unmarshal(x, &y)
	if err != nil {
		return nil, err
	}
	res.Doc = y
	return &res, nil
}

func (mw *MongoWorker) mongoRunCommandOp(client *mongo.Client, op *MongoRunCommandOp) {
	mw.fs = append(mw.fs, func() {
		db := client.Database(op.DbName)
		var result bson.M
		err := db.RunCommand(contextByName[op.Ctx], op.Doc).Decode(&result)
		if op.MakeLogs {
			if err != nil {
				tracelog.InfoLogger.Printf("cannot execute runCommand: %+v", err)
			} else {
				tracelog.InfoLogger.Printf("Successful execution of runCommand with %+v argument", op.Doc)
			}
		}
		<-mw.ch
		mw.wg.Done()
	})
}

func (mw *MongoWorker) run() {
	for _, f := range mw.fs {
		mw.ch <- struct{}{}
		mw.wg.Add(1)
		go f()
	}
	mw.wg.Wait()
}

func multiplyInArray(value string, cnt int) []string {
	var res []string
	for cnt > 0 {
		res = append(res, value)
		cnt--
	}
	return res
}

func main() {

	cli, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		fmt.Println("cannot create client to mongodb: %v", err)
		return
	}
	err = cli.Connect(context.TODO())
	if err != nil {
		fmt.Println("cannot connect to mongodb: %v", err)
	}
	err = cli.Ping(context.TODO(), nil)
	if err != nil {
		fmt.Println("cannot ping mongodb basse: %v", err)
		return
	}

	mw := CreateMongoWorker(1)

	jsonstrins := `{"op":"c", "db":"testName1","makelogs":true, "dc":{"insert":"testName2", "documents":[{"Key":"name1", "sub":"asdf"},{"Value":"Alice"}]}}`
	jsonstrdel := `{"op":"c", "db":"testName1", "makelogs":true, "dc":{"delete":"testName2", "deletes":[{"q": {"Key":"name1"}, "limit":1}]}}`
	jsonfind := `{"op":"c", "db":"testName1", "ctx":"todo", "dc":{"find":"testName2"}, "makelogs": true}`

	err = mw.addMongoOp(cli, "["+strings.Join(multiplyInArray(jsonstrins, 3), ",")+"]")
	if err != nil {
		fmt.Println(err)
	}

	err = mw.addMongoOp(cli, "["+jsonstrdel+"]")
	if err != nil {
		fmt.Println(err)
	}

	err = mw.addMongoOp(cli, "["+jsonfind+"]")
	if err != nil {
		fmt.Println(err)
	}

	mw.run()

	cur, err := cli.Database("testName1").Collection("testName2").Find(context.TODO(), bson.M{})
	if err != nil {
		fmt.Println(err)
	}
	if cur != nil {
		for cur.Next(context.TODO()) {
			fmt.Println(cur.Current)
		}
	}
}
