import './App.css';

console.log("Application has started.");

function App() {
  return (
    <div className="App">
      <div className="toolbar" role="banner">
        <img width="40" height=" 30" alt="JPMC Logo PLaceholder"
          src="https://th.bing.com/th/id/OIP.42US39t41lKv7d-hqR7ImQHaE8?pid=ImgDet&rs=1"></img>

        <h2>Cassandra Clusters Application</h2>

        <div className="spacer"></div>

      </div>

      <div className="row">
        <div className="column1">
          <div className="content" role="main">

            <h2>Table Information:</h2>

            <input type="file" accept=".csv" id="fileUpload" onClick={csvtoJS} />
            <table id="table-stats">
              <tr>
                <th>Keyspace Name</th>
                <th>Table Name</th>
                <th>Num Partitions</th>
                <th>Partition Row Stats</th>
                <th>Column Definitions</th>
                <th>Table Size</th>
                <th>Partition Size Stats</th>
              </tr>
            </table>
            {/* <input type="button" id="upload" value="Upload" onclick={Upload} /> */}
            {/* <div id="dvCSV">
              <div id="dvReplace">
              </div>
            </div> */}


          </div>
        </div>
        <div class="column2">

        </div>
        <div class="column3">
          <div class="sidebar-menu">
            <div class="logo">
              <br></br>
              <br></br>
              <br></br>
              <br></br>
              <h2>Cluster Information</h2>
            </div>
            <section class="container">
              {/* <div class="one">
                <ul>
                  <li>
                    <p>Cluster Name:</p>
                  </li>
                  <li>
                    <p>Number of Tables:</p>
                  </li>
                  <li>
                    <p>User Generated Tables:</p>
                  </li>
                  <li>
                    <p>Cluster Size:</p>
                  </li>
                </ul>
              </div>
              <div class="two">
                <ul>
                  <li>
                    <p>D7329D209426CC3CAF298FC6CBC2B52B6A707BFC</p>
                  </li>
                  <li>
                    <p>D7329D209426CC3CAF298FC6CBC2B52B6A707BFC</p>
                  </li>
                  <li>
                    <p>D7329D209426CC3CAF298FC6CBC2B52B6A707BFC</p>
                  </li>
                  <li>
                    <p>D7329D209426CC3CAF298FC6CBC2B52B6A707BFC</p>
                  </li>
                </ul>
              </div> */}

              <input type="file" name="inputfile" id="inputfile"></input>
              <br></br>

              <pre id="output"></pre>

            </section>
          </div>
        </div>

      </div>
    </div >
  );
}

/*
function Upload() {
  var fileUpload = document.getElementById("fileUpload");
  var regex = /^([a-zA-Z0-9\s_\\.\-:])+(.csv|.txt)$/;
  if (regex.test(fileUpload.value.toLowerCase())) {
    if (typeof (FileReader) != "undefined") {
      var reader = new FileReader();
      reader.onload = function (e) {
        var table = document.createElement("table");
        var rows = e.target.result.split("\n");
        for (var i = 1; i < rows.length; i++) {
          var cells = rows[i].split(",");
          if (cells.length > 1) {
            var row = table.insertRow(-1);
            for (var j = 0; j < cells.length; j++) {
              var cell = row.insertCell(-1);
              cell.innerHTML = cells[j];
            }
          }
        }
        var dvCSV = document.getElementById("dvCSV");
        dvCSV.innerHTML = "";
        dvCSV.appendChild(table);
      }
      reader.readAsText(fileUpload.value[0]);
    } else {
      alert("This browser does not support HTML5.");
    }
  } else {
    alert("Please upload a valid CSV file.");
  }
}
*/

function csvtoJS() {
  const x = document.querySelector("input");

  x.addEventListener("change", () => {

    const fr = new FileReader();

    fr.onloadend = e => {

      let r = fr.result.split("\n").map(e => {
        return e.split(",")
      });

      r.forEach(e => {

        let m = e.map(e => {
          return `<td>${e}</td>`;
        }).join("");

        console.log(m);
        console.log(r);

        const ce = document.createElement("tr");

        ce.innerHTML = m;

        if (ce.innerText !== "") {
          document.querySelector("table").append(ce);
        }

      });
    }
    fr.readAsText(x.files[0]);

  })
}

window.onload = function () {
  var t = document.getElementById('inputfile');

  if (t) {
    t.addEventListener('change', () => {

      var fr = new FileReader();
      fr.onload = function () {
        document.getElementById('output').textContent = fr.result;
      }
      fr.readAsText(t.files[0]);
    })
  } else {
    alert("ID not found.");
  }
}




export default App;
