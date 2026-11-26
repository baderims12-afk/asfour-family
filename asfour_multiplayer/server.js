const db=require('./public/questions.json');
function shuffle(a){return [...a].sort(()=>0).map(x=>[Math.random(),x]).sort((a,b)=>a[0]-b[0]).map(x=>x[1])}
function reset(r){r.cells=Array(25).fill('');r.selected=null;r.winner=null;r.letters=shuffle(db.letters).slice(0,25);r.questionIndex=r.letters.map(l=>{const n=db.bank.filter(x=>x.letter===l).length;return ((r.used?.[l]??-1)+1)%n});r.used={...r.used};r.letters.forEach((l,i)=>r.used[l]=r.questionIndex[i]);}
const express=require('express'),{WebSocketServer,WebSocket}=require('ws'),http=require('http'),crypto=require('crypto');
const app=express();app.use(express.static('public'));app.get('/health',(req,res)=>res.json({ok:true}));
const server=http.createServer(app),wss=new WebSocketServer({server});const rooms=new Map();
const send=(ws,obj)=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj))};
function broadcast(room){for(const ws of room.clients)send(ws,{type:'state',code:room.code,cells:room.cells,letters:room.letters,selected:room.selected,questionIndex:room.questionIndex,host:ws.id===room.host,players:room.clients.size,winner:room.winner})}
// Green connects top to bottom; orange connects left to right.
// Touching tiles can connect horizontally, vertically, or diagonally.
function win(cells, team) {
  const queue = [], visited = new Set();
  for (let i = 0; i < 25; i++) {
    const row = Math.floor(i / 5), col = i % 5;
    if (cells[i] === team && (team === 'g' ? row === 0 : col === 0)) {
      queue.push(i); visited.add(i);
    }
  }
  for (let head = 0; head < queue.length; head++) {
    const i = queue[head], row = Math.floor(i / 5), col = i % 5;
    if (team === 'g' ? row === 4 : col === 4) return true;
    for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) {
      if (Math.abs(dr)+Math.abs(dc)!==1) continue;
      const nr = row + dr, nc = col + dc;
      if (nr < 0 || nr >= 5 || nc < 0 || nc >= 5) continue;
      const j = nr * 5 + nc;
      if (cells[j] === team && !visited.has(j)) { visited.add(j); queue.push(j); }
    }
  }
  return false;
}
wss.on('connection',ws=>{ws.id=crypto.randomUUID();ws.isAlive=true;ws.on('pong',()=>ws.isAlive=true);
ws.on('message',raw=>{if(raw.length>4096)return;let msg;try{msg=JSON.parse(raw)}catch{return}const room=rooms.get(ws.room);
if(msg.type==='create'&&!ws.room){let code;do{code=crypto.randomBytes(3).toString('hex').toUpperCase()}while(rooms.has(code));let r={code,clients:new Set([ws]),host:ws.id,cells:Array(25).fill(''),selected:null,questionIndex:Array(25).fill(0),winner:null};reset(r);rooms.set(code,r);ws.room=code;broadcast(r)}
else if(msg.type==='join'&&!ws.room){let code=String(msg.code||'').trim().toUpperCase(),r=rooms.get(code);if(!r)return send(ws,{type:'error',message:'رمز الغرفة غير صحيح'});if(r.clients.size>=30)return send(ws,{type:'error',message:'الغرفة ممتلئة'});r.clients.add(ws);ws.room=code;broadcast(r)}
else if(room&&ws.id===room.host){
if(msg.type==='select'&&Number.isInteger(msg.i)&&msg.i>=0&&msg.i<25&&!room.cells[msg.i]&&!room.winner)room.selected=msg.i;
if(msg.type==='next'&&room.selected!==null){const i=room.selected,l=room.letters[i];room.questionIndex[i]=(room.questionIndex[i]+1)%db.bank.filter(x=>x.letter===l).length;room.used[l]=room.questionIndex[i]}
if(msg.type==='claim'&&room.selected!==null&&['g','o'].includes(msg.team)&&!room.winner){room.cells[room.selected]=msg.team;room.winner=win(room.cells,msg.team)?msg.team:null;room.selected=null;if(room.winner){clearTimeout(room.timer);room.timer=setTimeout(()=>{if(rooms.get(room.code)===room){reset(room);broadcast(room)}},6000)}}
if(msg.type==='close')room.selected=null;
if(msg.type==='reset'){clearTimeout(room.timer);reset(room)}
broadcast(room)}
});
ws.on('close',()=>{let r=rooms.get(ws.room);if(!r)return;r.clients.delete(ws);if(!r.clients.size)rooms.delete(r.code);else{if(r.host===ws.id)r.host=[...r.clients][0].id;broadcast(r)}})});
setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}},30000);
server.listen(process.env.PORT||3000,'0.0.0.0',()=>console.log('Listening'));
