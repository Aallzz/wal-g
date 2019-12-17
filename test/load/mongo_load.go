package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tinsane/tracelog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type MongoInsertOp struct {
	MongoInsertRaw

	insertOneOpts  *options.InsertOneOptions
	insertManyOpts *options.InsertManyOptions
	makeLogs       bool
	logs           chan string
}

type MongoInsertRaw struct {
	DbName  string            `json:"db"`
	ColName string            `json:"cl"`
	Ctx     string            `json:"ctx"`
	Docs    []interface{}     `json:"docs"`
	Opts    map[string]string `json:"opts"`
}

func (mir *MongoInsertRaw) transformMongoInsertRaw() (*MongoInsertOp, error) {
	var res MongoInsertOp
	res.MongoInsertRaw = *mir
	res.insertOneOpts = &options.InsertOneOptions{}
	res.insertManyOpts = &options.InsertManyOptions{}

	if value, ok := mir.Opts["BypassDocumentValidation"]; ok {
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		res.insertOneOpts.BypassDocumentValidation = &boolValue
		res.insertManyOpts.BypassDocumentValidation = &boolValue
	}
	if value, ok := mir.Opts["Ordered"]; ok {
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		res.insertManyOpts.Ordered = &boolValue
	}
	if value, ok := mir.Opts["MakeLogs"]; ok {
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		res.makeLogs = boolValue
	}
	return &res, nil
}

func parseInsertOp(opdata map[string]interface{}) (*MongoInsertOp, error) {
	mio := MongoInsertRaw{}
	x, _ := json.Marshal(opdata)
	err := json.Unmarshal(x, &mio)
	if err != nil {
		return nil, fmt.Errorf("error in parsing insert operation: %v", err)
	}
	return mio.transformMongoInsertRaw()
}

type MongoDeleteOp struct {
	MongoDeleteRaw

	many     bool
	opts     *options.DeleteOptions
	makeLogs bool
	logs     chan string
}

type MongoDeleteRaw struct {
	DbName  string                 `json:"db"`
	ColName string                 `json:"cl"`
	Ctx     string                 `json:"ctx"`
	Filter  interface{}            `json:"filter"`
	Opts    map[string]interface{} `json:"opts"`
}

func (mdr *MongoDeleteRaw) transformMongoDeleteRaw() (*MongoDeleteOp, error) {
	var res MongoDeleteOp
	res.MongoDeleteRaw = *mdr
	res.opts = &options.DeleteOptions{}
	if value, ok := mdr.Opts["Collation"]; ok {
		x, _ := json.Marshal(value)
		err := json.Unmarshal(x, &res.opts.Collation)
		if err != nil {
			return nil, err
		}
	}
	if value, ok := mdr.Opts["MakeLogs"]; ok {
		strBoolValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("error in transforming MondoDeleteRaw type to MongoDeleteOp type: expected string for MakeLogs opt")
		}
		boolValue, err := strconv.ParseBool(strBoolValue)
		if err != nil {
			return nil, err
		}
		res.makeLogs = boolValue
	}
	if value, ok := mdr.Opts["Many"]; ok {
		strBoolValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("error in transforming MondoDeleteRaw type to MongoDeleteOp type: expected string for Many opt")
		}
		boolValue, err := strconv.ParseBool(strBoolValue)
		if err != nil {
			return nil, err
		}
		res.many = boolValue
	}
	return &res, nil
}

func parseDeleteOp(opdata map[string]interface{}) (*MongoDeleteOp, error) {
	mdo := MongoDeleteRaw{}
	x, _ := json.Marshal(opdata)
	err := json.Unmarshal(x, &mdo)
	if err != nil {
		return nil, fmt.Errorf("error in parsing delete operation: %v", err)
	}
	return mdo.transformMongoDeleteRaw()
}

var contextByName = map[string]context.Context{
	"todo":       context.TODO(),
	"background": context.Background(),
}

func mongoInsert(mongoClient *mongo.Client, op *MongoInsertOp) {
	collection := mongoClient.Database(op.DbName).Collection(op.ColName)
	if len(op.Docs) == 1 {
		inOneRes, err := collection.InsertOne(contextByName[op.Ctx], op.Docs[0], op.insertOneOpts)
		if op.makeLogs {
			var msg string
			if err != nil {
				msg = fmt.Sprintf("Failed insertion of one document in mongo database %s in %s collection with error: %v", op.DbName, op.ColName, err)
			} else {
				msg = fmt.Sprintf(`Successfull insertion of one document with id "%v" in mongo database %s in %s collectoin`, inOneRes.InsertedID, op.DbName, op.ColName)
			}
			tracelog.InfoLogger.Println(msg)
		}
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		inManyRes, err := collection.InsertMany(contextByName[op.Ctx], op.Docs, op.insertManyOpts)
		if op.makeLogs {
			var msg string
			if err != nil {
				msg = fmt.Sprintf("Failed insertion of many documents in mongo database %s in %s collection with error: %v", op.DbName, op.ColName, err)
			} else {
				msg = fmt.Sprintf(`Successfull insertion of many documents with ids "%v" in mongo database %s in %s collectoin`, inManyRes.InsertedIDs, op.DbName, op.ColName)
			}
			tracelog.InfoLogger.Println(msg)
		}
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func mongoDelete(mongoClient *mongo.Client, op *MongoDeleteOp) {
	collection := mongoClient.Database(op.DbName).Collection(op.ColName)
	if op.many {
		delRes, err := collection.DeleteMany(contextByName[op.Ctx], op.Filter, op.opts)
		if op.makeLogs {
			var msg string
			if err != nil {
				msg = fmt.Sprintf("Failed deletion of many documents in mongo database %s in %s collection with error: %v", op.DbName, op.ColName, err)
			} else {
				x, _ := json.Marshal(op.Filter)
				msg = fmt.Sprintf(`Successfull deletion of %d document(s) in mongo database %s in %s collectoin with filter %+v`, delRes.DeletedCount, op.DbName, op.ColName, string(x))
			}
			tracelog.InfoLogger.Println(msg)
		}
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		delRes, err := collection.DeleteOne(contextByName[op.Ctx], op.Filter, op.opts)
		if op.makeLogs {
			var msg string
			if err != nil {
				msg = fmt.Sprintf("Failed deletion of many documents in mongo database %s in %s collection with error: %v", op.DbName, op.ColName, err)
			} else {
				x, _ := json.Marshal(op.Filter)
				msg = fmt.Sprintf(`Successfull deletion of %d document(s) in mongo database %s in %s collectoin with filter %+v`, delRes.DeletedCount, op.DbName, op.ColName, string(x))
			}
			tracelog.InfoLogger.Println(msg)
		}
		if err != nil {
			log.Fatalln(err)
		}
	}
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
		case "i":
			mio, err := parseInsertOp(x)
			if err != nil {
				return err
			}
			mw.mongoInsertOp(client, mio)
			break
		case "d":
			mdo, err := parseDeleteOp(x)
			if err != nil {
				return err
			}
			mw.mongoDeleteOp(client, mdo)
			break
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
	DbName string
	Ctx    string
	Doc    bson.D
}

func parseCommandOp(cd map[string]interface{}) (*MongoRunCommandOp, error) {
	var res MongoRunCommandOp
	var ok bool
	res.DbName, ok = cd["db"].(string)
	if !ok {
		return nil, fmt.Errorf("error1")
	}
	res.Ctx, ok = cd["db"].(string)
	if !ok {
		return nil, fmt.Errorf("error2")
	}
	x, err := bson.Marshal(cd["doc"])
	if err != nil {
		return nil, err
	}
	var y bson.D
	err = bson.Unmarshal(x, &y)

	res.Doc = y
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func (mw *MongoWorker) mongoRunCommandOp(client *mongo.Client, op *MongoRunCommandOp) {
	mw.fs = append(mw.fs, func() {
		db := client.Database(op.DbName)
		var result bson.M
		_ = db.RunCommand(contextByName[op.Ctx], op.Doc).Decode(&result)
		<-mw.ch
		mw.wg.Done()
	})
}

func (mw *MongoWorker) mongoInsertOp(client *mongo.Client, op *MongoInsertOp) {
	mw.fs = append(mw.fs, func() {
		mongoInsert(client, op)
		<-mw.ch
		mw.wg.Done()
	})
}

func (mw *MongoWorker) mongoDeleteOp(client *mongo.Client, op *MongoDeleteOp) {
	mw.fs = append(mw.fs, func() {
		mongoDelete(client, op)
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

	jsonstrins := `{"op":"i", "db":"testName1","cl":"testName2","ctx":"todo","docs":[{"Key":"name1", "sub":"asdf"},{"Value":"Alice"}],"opts":{"MakeLogs":"true"}}`
	jsonstrdel := `{"op":"d", "db":"testName1","cl":"testName2","ctx":"todo","many":"true","filter":{"Key":"name1"},"opts":{"Locale":"fr","Many":"true","MakeLogs":"true"}}`
	jsoncmd := `{"op":"c", "db":"testName1", "ctx":"todo", "doc":{"explain":{"find":"testName2"}}}`
	jsonfind := `{"op":"c", "db":"testName1", "ctx":"todo", "doc":{"find":"testName2"}}`

	err = mw.addMongoOp(cli, "["+strings.Join(multiplyInArray(jsonstrins, 3), ",")+"]")
	if err != nil {
		fmt.Println(err)
	}

	err = mw.addMongoOp(cli, "["+jsonstrdel+"]")
	if err != nil {
		fmt.Println(err)
	}

	err = mw.addMongoOp(cli, "["+ jsonfind +"]")
	if err != nil {
		fmt.Println(err)
	}

	err = mw.addMongoOp(cli, "["+ jsoncmd +"]")
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
