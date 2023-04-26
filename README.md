<h1>Go Mongo Update Script</h1>

<h4>This script read data from file and update it in mongo database</h4>

<p>The <b>readJSONFile()</b> function reads the data from the JSON file and returns the records in the file as a slice of Record structs.</p>

<p>The <b>connect()</b> function establishes a connection to the MongoDB database and returns a mongo.Client, context.Context, context.CancelFunc, and an error.
The ping() function is used to check if the connection is successful.</p>

<p>The <b>findAndValidate()</b> function is used to find documents based on a query and validate if the total documents filtered are greater than the maximum limit.
The maximum limit is set as a constant MaxMatchLimit.</p>

<p>The <b>updateOne()</b> function is used to update a single document that matches the filter with the provided update.</p>

<p>In the <b>main()</b> function, the program reads the JSON file using the <b>readJSONFile()</b> function and establishes a connection to the database using <b>connect()</b> function. 
It then loops through each record in the file and updates the corresponding document in the collection using the <b>updateOne()</b> function. 
The <b>findAndValidate()</b> function is used to validate if the document should be updated. 
The total matched and modified count is calculated and printed at the end.</p>

<h3>Contact the author</h3>
<p>Email: kashifkhan_9@yahoo.com</p>