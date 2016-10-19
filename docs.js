var lang = {
  "DEFAULT_VERSION": "Python", 
  "VERSIONS":{
    "Scala":{
      "type": "Production",
      "version": ""
    },
    "Python":{
      "type": "Production",
      "version": ""
    }
  }
}
var VERSIONS = {};
var DEFAULT_VERSION;
$(document).ready(function(){
try{
  $.ajax({
  url: "versions.json",
  dataType:"json"
  })
  .success(function(data, jqXHR){
    console.log(data);
    VERSIONS = data.VERSIONS;
    DEFAULT_VERSION = data.DEFAULT_VERSION
    console.log(VERSIONS);
    updateIframeSrc(data.DEFAULT_VERSION);
    appendMenu(".major-menu", makeMenu(data.DEFAULT_VERSION, VERSIONS),bindClick_version );
  });
}catch(err){}

console.log(lang)
console.log(makeMenu(lang.DEFAULT_VERSION, lang))
appendMenu(".doc-type-major-menu",makeMenu(lang.DEFAULT_VERSION, lang.VERSIONS), bindClick_doc)
});

function updateIframeSrc(selected, menu){

  version = $(".major-menu .top").first().attr("id")
  if (version == undefined){
    version = DEFAULT_VERSION
  }
  doc_type = $(".doc-type-major-menu .top").first().attr("id").toLocaleLowerCase()
  if (menu == "doc"){
    doc_type = selected.toLocaleLowerCase();
  }
  else if( menu == "version"){
    version = selected
  }
  console.log(selected, version, doc_type, "update url")
  $("#doc-content").attr("src", "versions/"+version+"/"+doc_type+"/");
}

bindClick_version = function(class_name){
  $(".major-menu .doc").unbind("click");
  $(".major-menu .doc").click(function() {
    updateIframeSrc(this.id, "version");
    appendMenu(".major-menu", makeMenu(this.id, VERSIONS), bindClick_version);
  });
}
bindClick_doc = function(class_name){
  $(".doc-type-major-menu .doc").unbind("click");
  $(".doc-type-major-menu .doc").click(function() {
    updateIframeSrc(this.id, "doc");
    console.log(this.id)
    appendMenu(".doc-type-major-menu", makeMenu(this.id, lang.VERSIONS), bindClick_doc);
  });
}

function makeMenu(selected, versions){
  var list = '<li id="'+selected+'" class="top"><a class="selected" id="'+selected+'" > '+selected+'</a>' +
             '<ul class="specific-version"> ';
  $.each( versions, function( index, value){
    if(selected != index){
      list += '<li id="'+index+'" class="bottom"><a class="doc" id="'+index+'" > '+index+'</a></li>';
    }
  });
  list += '</ul></li>';
  return list;
}


function appendMenu(class_name, list, bindClick){
  $(class_name).empty().append(list)
  bindClick();
}



