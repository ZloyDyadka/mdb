# mdb

Wrapper for MGO Driver with automatic reconnection and retries when connection is break.
Provides the entire API of the mgo driver

# feature

* auto refresh connections when connection is break
* more simple

# why this one

if you use
```go
copy := session.Clone();
defer copy.Close();
copy.DB("dbname).C("col").Find(...)
```

you may got "Closed explicitly" or "EOF"  when in high concurrency

# quick start

```go
type Person struct {
	Name string
	Phone string
}

func main() {
    //By default max retries is 2 and interval 2 seconds
	session, err := mdb.Dial("127.0.0.1:27017", mdb.MaxRetries(100), mdb.RetryInterval(time.Second * 2))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	c := session.DB("test").C("people")

	err = c.Insert(&Person{"Ale", "+55 53 8116 9639"},&Person{"Cla", "+55 53 8402 8510"})
	if err != nil {
		log.Fatal(err)
	}

	result := Person{}
	err = c.Find(bson.M{"name": "Ale"}).One(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Phone:", result.Phone)
}
```