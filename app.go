package main

import (
	"github.com/drone/routes"
	"log"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"github.com/mkilling/goejdb"
	"gopkg.in/mgo.v2/bson"
	"os"
	"github.com/naoina/toml"
	"net"
	"net/rpc"
	"strings"
)

type tomlConfig struct {
	Replication struct {
			    RpcServerPortNum int
			    Replica          []string
		    }
	Database    struct {
			    FileName string
			    PortNum  int
		    }
}

var rpc_server_port int
var replica string
var db_name string

type Listener int

func (l *Listener) GetLine(bsrec []byte, ack *bool) error {

	var jb, jb_err = goejdb.Open(db_name, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("personal", nil)

	if jb_err != nil {
		os.Exit(1)
	}

	oid, _ := coll.SaveBson(bsrec)
	fmt.Printf("\nSaved Record")
	fmt.Println(oid)
	jb.Close()

	return nil
}

func main() {
	//TOML Configuration
	if(len(os.Args) < 2){
		fmt.Println("Please Enter a toml config path")
		os.Exit(1)
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	var config tomlConfig
	if err := toml.Unmarshal(buf, &config); err != nil {
		panic(err)
	}
	//declaration for Replication
	db_name = config.Database.FileName
	rpc_server_port = config.Replication.RpcServerPortNum
	replica = config.Replication.Replica[0]

	if (strings.HasPrefix(replica, "http://")) {
		replica = replica[7:]
	}

	//Create and close the database
	// var jb,_ = goejdb.Open(db_name, goejdb.JBOCREAT)

	// jb, _ := goejdb.Open(db_name, goejdb.JBOWRITER | goejdb.JBOCREAT | goejdb.JBOTRUNC)
	var jb, _ = goejdb.Open(db_name, goejdb.JBOWRITER)
	jb.Close()

	mux := routes.New()
	mux.Get("/profile/:email", GetProfile)
	mux.Post("/profile", PostProfile)
	mux.Put("/profile/:email", PutProfile)
	mux.Del("/profile/:email", DeleteProfile)

	http.Handle("/", mux)
	log.Println("Listening...")
	//server creation
	var port = fmt.Sprintf(":%d", config.Database.PortNum)

	go func() {
		http.ListenAndServe(port, nil)
	}()

	addy, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%d", rpc_server_port))
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}

	listener := new(Listener)
	rpc.Register(listener)
	rpc.Accept(inbound)

}

func GetProfile(w http.ResponseWriter, req *http.Request) {
	var jb, err = goejdb.Open(db_name, goejdb.JBOREADER)

	if err != nil {
		log.Fatal(err)
	}

	var coll, _ = jb.CreateColl("personal", nil)
	params := req.URL.Query()                        //Get all the request parameters from the URL
	email := params.Get(":email")                    //Get the emailid from the URL

	// Now execute queryzz
	query := fmt.Sprintf(`{"email" : "%s"}`, email)
	res, err := coll.Find(query) 

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\n\nRecords found: %d\n", len(res))

	var m map[string]interface{}

	if (len(res) > 0) {
		bson.Unmarshal(res[0],&m)
		mapB, _ := json.Marshal(m)     //Get the profile object corresponding to email key and marshal it to JSON
		w.Write([]byte(mapB))				 //Write the JSON to response
	} else {
		w.Write([]byte("Email does not exist."))
	}
	jb.Close()
}

func PostProfile(rw http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)		//Read the http body
	if err != nil {
		log.Println(err.Error())
	}

	var profileMap map[string]interface{}
	err = json.Unmarshal(body, &profileMap)	       //Unmarshall the json into a map
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(profileMap)

	var jb, jb_err = goejdb.Open(db_name, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("personal", nil)

	if jb_err != nil {
		os.Exit(1)
	}

	// Now execute queryzz
	query := fmt.Sprintf(`{"email" : "%s"}`, profileMap["email"])
	res, _ := coll.Find(query) // Name starts with 'Bru' string
	fmt.Printf("\n\nRecords found: %d\n", len(res))

	if (len(res) == 0) {
		// Insert one record:
		bsrec, _ := bson.Marshal(profileMap)
		coll.SaveBson(bsrec)
		fmt.Printf("\nSaved Record")
		rw.WriteHeader(http.StatusCreated)

		client, err := rpc.Dial("tcp", replica)
		if err != nil {
			log.Fatal(err)
		}

		var reply bool

		err = client.Call("Listener.GetLine", bsrec, &reply)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println("Record Already Exist.")
		rw.Write([]byte("Record Already Exist."))
	}
	jb.Close()

}

func PutProfile(rw http.ResponseWriter, req *http.Request) {
	params := req.URL.Query()                        //Get all the request parameters from the URL
	email := params.Get(":email")                    //Get the emailid from the URL

	var jb, _ = goejdb.Open(db_name, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("personal", nil)

	// Now execute queryzz
	query := fmt.Sprintf(`{"email" : "%s"}`, email) //check the match for email
	res, _ := coll.Find(query) // Match Email
	fmt.Printf("\n\nRecords found: %d\n", len(res))

	if (len(res) > 0) {                                    //if a record match is found
		var currentProfileMap map[string]interface{}   //create a map and convert the json to string to store it in the map
		bson.Unmarshal(res[0],&currentProfileMap)

		body, err := ioutil.ReadAll(req.Body)		//Read the http body (json format)
		log.Println(string(body))

		if err != nil {
			log.Println(err.Error())
		}

		var newProfilemap map[string]interface{}
		err = json.Unmarshal(body, &newProfilemap)	 //Unmarshall the json into a map
		if err != nil {
			log.Println(err.Error())
		}
		log.Println(newProfilemap)

		for key, value := range newProfilemap {		//for each key, value in the new profile (from put body), update the corresponding key in already existing profile
			log.Println(string(key))
			log.Println(value)
			currentProfileMap[key] = value
		}

		bsrec, _ := bson.Marshal(currentProfileMap)
		coll.SaveBson(bsrec)
		fmt.Printf("\nSaved Record")
		rw.WriteHeader(http.StatusNoContent)

		client, err := rpc.Dial("tcp", replica)
		if err != nil {
			log.Fatal(err)
		}

		var reply bool

		err = client.Call("Listener.GetLine", bsrec, &reply)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		rw.Write([]byte("Email does not exist."))
	}
	jb.Close()

}

func DeleteProfile(w http.ResponseWriter, req *http.Request) {
	params := req.URL.Query()
	email := params.Get(":email")

	var jb, jb_err = goejdb.Open(db_name, goejdb.JBOWRITER)
	var coll, _ = jb.CreateColl("personal", nil)

	if jb_err != nil {
		os.Exit(1)
	}
	query := fmt.Sprintf(`{"email" : "%s"}`, email) //check the match for email
	res, _ := coll.Find(query) // Match Email
	fmt.Printf("\n\nRecords found: %d\n", len(res))
	if (len(res) > 0) {
		var currentProfileMap map[string]interface{}   //create a map and convert the json to string to store it in the map
		bson.Unmarshal(res[0],&currentProfileMap)

		log.Println(currentProfileMap)
		objid, _ := currentProfileMap["_id"].(bson.ObjectId)

		coll.RmBson(objid.Hex())
	}
	jb.Close()
	w.WriteHeader(http.StatusNoContent)
}
