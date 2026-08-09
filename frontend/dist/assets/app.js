import{i as Je,r as b,a as He,A as _,b as u,t as Qe}from"./state.js";const Ve={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},Be={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},ae={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},le={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},Ge={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'},Ke={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},ce={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const Se=Object.freeze({left:0,top:0,width:16,height:16}),U=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),R=Object.freeze({...Se,...U}),X=Object.freeze({...R,body:"",hidden:!1}),We=Object.freeze({width:null,height:null}),Te=Object.freeze({...We,...U});function Ye(e,i=0){const t=e.replace(/^-?[0-9.]*/,"");function s(n){for(;n<0;)n+=4;return n%4}if(t===""){const n=parseInt(e);return isNaN(n)?0:s(n)}else if(t!==e){let n=0;switch(t){case"%":n=25;break;case"deg":n=90}if(n){let o=parseFloat(e.slice(0,e.length-t.length));return isNaN(o)?0:(o=o/n,o%1===0?s(o):0)}}return i}const Xe=/[\s,]+/;function Ze(e,i){i.split(Xe).forEach(t=>{switch(t.trim()){case"horizontal":e.hFlip=!0;break;case"vertical":e.vFlip=!0;break}})}const Ie={...Te,preserveAspectRatio:""};function de(e){const i={...Ie},t=(s,n)=>e.getAttribute(s)||n;return i.width=t("width",null),i.height=t("height",null),i.rotate=Ye(t("rotate","")),Ze(i,t("flip","")),i.preserveAspectRatio=t("preserveAspectRatio",t("preserveaspectratio","")),i}function et(e,i){for(const t in Ie)if(e[t]!==i[t])return!0;return!1}const Ce=/^[a-z0-9]+(-[a-z0-9]+)*$/,N=(e,i,t,s="")=>{const n=e.split(":");if(e.slice(0,1)==="@"){if(n.length<2||n.length>3)return null;s=n.shift().slice(1)}if(n.length>3||!n.length)return null;if(n.length>1){const r=n.pop(),l=n.pop(),c={provider:n.length>0?n[0]:s,prefix:l,name:r};return i&&!M(c)?null:c}const o=n[0],a=o.split("-");if(a.length>1){const r={provider:s,prefix:a.shift(),name:a.join("-")};return i&&!M(r)?null:r}if(t&&s===""){const r={provider:s,prefix:"",name:o};return i&&!M(r,t)?null:r}return null},M=(e,i)=>e?!!((i&&e.prefix===""||e.prefix)&&e.name):!1;function tt(e,i){const t=e.icons,s=e.aliases||Object.create(null),n=Object.create(null);function o(a){if(t[a])return n[a]=[];if(!(a in n)){n[a]=null;const r=s[a]&&s[a].parent,l=r&&o(r);l&&(n[a]=[r].concat(l))}return n[a]}return Object.keys(t).concat(Object.keys(s)).forEach(o),n}function it(e,i){const t={};!e.hFlip!=!i.hFlip&&(t.hFlip=!0),!e.vFlip!=!i.vFlip&&(t.vFlip=!0);const s=((e.rotate||0)+(i.rotate||0))%4;return s&&(t.rotate=s),t}function ue(e,i){const t=it(e,i);for(const s in X)s in U?s in e&&!(s in t)&&(t[s]=U[s]):s in i?t[s]=i[s]:s in e&&(t[s]=e[s]);return t}function nt(e,i,t){const s=e.icons,n=e.aliases||Object.create(null);let o={};function a(r){o=ue(s[r]||n[r],o)}return a(i),t.forEach(a),ue(e,o)}function De(e,i){const t=[];if(typeof e!="object"||typeof e.icons!="object")return t;e.not_found instanceof Array&&e.not_found.forEach(n=>{i(n,null),t.push(n)});const s=tt(e);for(const n in s){const o=s[n];o&&(i(n,nt(e,n,o)),t.push(n))}return t}const st={provider:"",aliases:{},not_found:{},...Se};function K(e,i){for(const t in i)if(t in e&&typeof e[t]!=typeof i[t])return!1;return!0}function je(e){if(typeof e!="object"||e===null)return null;const i=e;if(typeof i.prefix!="string"||!e.icons||typeof e.icons!="object"||!K(e,st))return null;const t=i.icons;for(const n in t){const o=t[n];if(!n||typeof o.body!="string"||!K(o,X))return null}const s=i.aliases||Object.create(null);for(const n in s){const o=s[n],a=o.parent;if(!n||typeof a!="string"||!t[a]&&!s[a]||!K(o,X))return null}return i}const J=Object.create(null);function rt(e,i){return{provider:e,prefix:i,icons:Object.create(null),missing:new Set}}function S(e,i){const t=J[e]||(J[e]=Object.create(null));return t[i]||(t[i]=rt(e,i))}function Pe(e,i){return je(i)?De(i,(t,s)=>{s?e.icons[t]=s:e.missing.add(t)}):[]}function ot(e,i,t){try{if(typeof t.body=="string")return e.icons[i]={...t},!0}catch{}return!1}function at(e,i){let t=[];return(typeof e=="string"?[e]:Object.keys(J)).forEach(s=>{(typeof s=="string"&&typeof i=="string"?[i]:Object.keys(J[s]||{})).forEach(n=>{const o=S(s,n);t=t.concat(Object.keys(o.icons).map(a=>(s!==""?"@"+s+":":"")+n+":"+a))})}),t}let E=!1;function Ae(e){return typeof e=="boolean"&&(E=e),E}function L(e){const i=typeof e=="string"?N(e,!0,E):e;if(i){const t=S(i.provider,i.prefix),s=i.name;return t.icons[s]||(t.missing.has(s)?null:void 0)}}function Oe(e,i){const t=N(e,!0,E);if(!t)return!1;const s=S(t.provider,t.prefix);return i?ot(s,t.name,i):(s.missing.add(t.name),!0)}function he(e,i){if(typeof e!="object")return!1;if(typeof i!="string"&&(i=e.provider||""),E&&!i&&!e.prefix){let s=!1;return je(e)&&(e.prefix="",De(e,(n,o)=>{Oe(n,o)&&(s=!0)})),s}const t=e.prefix;return M({prefix:t,name:"a"})?!!Pe(S(i,t),e):!1}function lt(e){return!!L(e)}function ct(e){const i=L(e);return i&&{...R,...i}}function Ee(e,i){e.forEach(t=>{const s=t.loaderCallbacks;s&&(t.loaderCallbacks=s.filter(n=>n.id!==i))})}function dt(e){e.pendingCallbacksFlag||(e.pendingCallbacksFlag=!0,setTimeout(()=>{e.pendingCallbacksFlag=!1;const i=e.loaderCallbacks?e.loaderCallbacks.slice(0):[];if(!i.length)return;let t=!1;const s=e.provider,n=e.prefix;i.forEach(o=>{const a=o.icons,r=a.pending.length;a.pending=a.pending.filter(l=>{if(l.prefix!==n)return!0;const c=l.name;if(e.icons[c])a.loaded.push({provider:s,prefix:n,name:c});else if(e.missing.has(c))a.missing.push({provider:s,prefix:n,name:c});else return t=!0,!0;return!1}),a.pending.length!==r&&(t||Ee([e],o.id),o.callback(a.loaded.slice(0),a.missing.slice(0),a.pending.slice(0),o.abort))})}))}let ut=0;function ht(e,i,t){const s=ut++,n=Ee.bind(null,t,s);if(!i.pending.length)return n;const o={id:s,icons:i,callback:e,abort:n};return t.forEach(a=>{(a.loaderCallbacks||(a.loaderCallbacks=[])).push(o)}),n}function pt(e){const i={loaded:[],missing:[],pending:[]},t=Object.create(null);e.sort((n,o)=>n.provider!==o.provider?n.provider.localeCompare(o.provider):n.prefix!==o.prefix?n.prefix.localeCompare(o.prefix):n.name.localeCompare(o.name));let s={provider:"",prefix:"",name:""};return e.forEach(n=>{if(s.name===n.name&&s.prefix===n.prefix&&s.provider===n.provider)return;s=n;const o=n.provider,a=n.prefix,r=n.name,l=t[o]||(t[o]=Object.create(null)),c=l[a]||(l[a]=S(o,a));let d;r in c.icons?d=i.loaded:a===""||c.missing.has(r)?d=i.missing:d=i.pending;const f={provider:o,prefix:a,name:r};d.push(f)}),i}const Z=Object.create(null);function pe(e,i){Z[e]=i}function ee(e){return Z[e]||Z[""]}function ft(e,i=!0,t=!1){const s=[];return e.forEach(n=>{const o=typeof n=="string"?N(n,i,t):n;o&&s.push(o)}),s}function se(e){let i;if(typeof e.resources=="string")i=[e.resources];else if(i=e.resources,!(i instanceof Array)||!i.length)return null;return{resources:i,path:e.path||"/",maxURL:e.maxURL||500,rotate:e.rotate||750,timeout:e.timeout||5e3,random:e.random===!0,index:e.index||0,dataAfterTimeout:e.dataAfterTimeout!==!1}}const Q=Object.create(null),P=["https://api.simplesvg.com","https://api.unisvg.com"],q=[];for(;P.length>0;)P.length===1||Math.random()>.5?q.push(P.shift()):q.push(P.pop());Q[""]=se({resources:["https://api.iconify.design"].concat(q)});function fe(e,i){const t=se(i);return t===null?!1:(Q[e]=t,!0)}function V(e){return Q[e]}function gt(){return Object.keys(Q)}const mt={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function bt(e,i,t,s){const n=e.resources.length,o=e.random?Math.floor(Math.random()*n):e.index;let a;if(e.random){let h=e.resources.slice(0);for(a=[];h.length>1;){const k=Math.floor(Math.random()*h.length);a.push(h[k]),h=h.slice(0,k).concat(h.slice(k+1))}a=a.concat(h)}else a=e.resources.slice(o).concat(e.resources.slice(0,o));const r=Date.now();let l="pending",c=0,d,f=null,m=[],x=[];typeof s=="function"&&x.push(s);function T(){f&&(clearTimeout(f),f=null)}function I(){l==="pending"&&(l="aborted"),T(),m.forEach(h=>{h.status==="pending"&&(h.status="aborted")}),m=[]}function y(h,k){k&&(x=[]),typeof h=="function"&&x.push(h)}function B(){return{startTime:r,payload:i,status:l,queriesSent:c,queriesPending:m.length,subscribe:y,abort:I}}function C(){l="failed",x.forEach(h=>{h(void 0,d)})}function $(){m.forEach(h=>{h.status==="pending"&&(h.status="aborted")}),m=[]}function w(h,k,j){const F=k!=="success";switch(m=m.filter(D=>D!==h),l){case"pending":break;case"failed":if(F||!e.dataAfterTimeout)return;break;default:return}if(k==="abort"){d=j,C();return}if(F){d=j,m.length||(a.length?G():C());return}if(T(),$(),!e.random){const D=e.resources.indexOf(h.resource);D!==-1&&D!==e.index&&(e.index=D)}l="completed",x.forEach(D=>{D(j)})}function G(){if(l!=="pending")return;T();const h=a.shift();if(h===void 0){if(m.length){f=setTimeout(()=>{T(),l==="pending"&&($(),C())},e.timeout);return}C();return}const k={status:"pending",resource:h,callback:(j,F)=>{w(k,j,F)}};m.push(k),c++,f=setTimeout(G,e.rotate),t(h,i,k.callback)}return setTimeout(G),B}function Le(e){const i={...mt,...e};let t=[];function s(){t=t.filter(a=>a().status==="pending")}function n(a,r,l){const c=bt(i,a,r,(d,f)=>{s(),l&&l(d,f)});return t.push(c),c}function o(a){return t.find(r=>a(r))||null}return{query:n,find:o,setIndex:a=>{i.index=a},getIndex:()=>i.index,cleanup:s}}function ge(){}const W=Object.create(null);function vt(e){if(!W[e]){const i=V(e);if(!i)return;W[e]={config:i,redundancy:Le(i)}}return W[e]}function Re(e,i,t){let s,n;if(typeof e=="string"){const o=ee(e);if(!o)return t(void 0,424),ge;n=o.send;const a=vt(e);a&&(s=a.redundancy)}else{const o=se(e);if(o){s=Le(o);const a=ee(e.resources?e.resources[0]:"");a&&(n=a.send)}}return!s||!n?(t(void 0,424),ge):s.query(i,n,t)().abort}function me(){}function yt(e){e.iconsLoaderFlag||(e.iconsLoaderFlag=!0,setTimeout(()=>{e.iconsLoaderFlag=!1,dt(e)}))}function xt(e){const i=[],t=[];return e.forEach(s=>{(s.match(Ce)?i:t).push(s)}),{valid:i,invalid:t}}function A(e,i,t){function s(){const n=e.pendingIcons;i.forEach(o=>{n&&n.delete(o),e.icons[o]||e.missing.add(o)})}if(t&&typeof t=="object")try{if(!Pe(e,t).length){s();return}}catch(n){console.error(n)}s(),yt(e)}function be(e,i){e instanceof Promise?e.then(t=>{i(t)}).catch(()=>{i(null)}):i(e)}function wt(e,i){e.iconsToLoad?e.iconsToLoad=e.iconsToLoad.concat(i).sort():e.iconsToLoad=i,e.iconsQueueFlag||(e.iconsQueueFlag=!0,setTimeout(()=>{e.iconsQueueFlag=!1;const{provider:t,prefix:s}=e,n=e.iconsToLoad;if(delete e.iconsToLoad,!n||!n.length)return;const o=e.loadIcon;if(e.loadIcons&&(n.length>1||!o)){be(e.loadIcons(n,s,t),c=>{A(e,n,c)});return}if(o){n.forEach(c=>{be(o(c,s,t),d=>{A(e,[c],d?{prefix:s,icons:{[c]:d}}:null)})});return}const{valid:a,invalid:r}=xt(n);if(r.length&&A(e,r,null),!a.length)return;const l=s.match(Ce)?ee(t):null;if(!l){A(e,a,null);return}l.prepare(t,s,a).forEach(c=>{Re(t,c,d=>{A(e,c.icons,d)})})}))}const re=(e,i)=>{const t=pt(ft(e,!0,Ae()));if(!t.pending.length){let r=!0;return i&&setTimeout(()=>{r&&i(t.loaded,t.missing,t.pending,me)}),()=>{r=!1}}const s=Object.create(null),n=[];let o,a;return t.pending.forEach(r=>{const{provider:l,prefix:c}=r;if(c===a&&l===o)return;o=l,a=c,n.push(S(l,c));const d=s[l]||(s[l]=Object.create(null));d[c]||(d[c]=[])}),t.pending.forEach(r=>{const{provider:l,prefix:c,name:d}=r,f=S(l,c),m=f.pendingIcons||(f.pendingIcons=new Set);m.has(d)||(m.add(d),s[l][c].push(d))}),n.forEach(r=>{const l=s[r.provider][r.prefix];l.length&&wt(r,l)}),i?ht(i,t,n):me},kt=e=>new Promise((i,t)=>{const s=typeof e=="string"?N(e,!0):e;if(!s){t(e);return}re([s||e],n=>{if(n.length&&s){const o=L(s);if(o){i({...R,...o});return}}t(e)})});function ve(e){try{const i=typeof e=="string"?JSON.parse(e):e;if(typeof i.body=="string")return{...i}}catch{}}function $t(e,i){if(typeof e=="object")return{data:ve(e),value:e};if(typeof e!="string")return{value:e};if(e.includes("{")){const o=ve(e);if(o)return{data:o,value:e}}const t=N(e,!0,!0);if(!t)return{value:e};const s=L(t);if(s!==void 0||!t.prefix)return{value:e,name:t,data:s};const n=re([t],()=>i(e,t,L(t)));return{value:e,name:t,loading:n}}let Ne=!1;try{Ne=navigator.vendor.indexOf("Apple")===0}catch{}function _t(e,i){switch(i){case"svg":case"bg":case"mask":return i}return i!=="style"&&(Ne||e.indexOf("<a")===-1)?"svg":e.indexOf("currentColor")===-1?"bg":"mask"}const St=/(-?[0-9.]*[0-9]+[0-9.]*)/g,Tt=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function te(e,i,t){if(i===1)return e;if(t=t||100,typeof e=="number")return Math.ceil(e*i*t)/t;if(typeof e!="string")return e;const s=e.split(St);if(s===null||!s.length)return e;const n=[];let o=s.shift(),a=Tt.test(o);for(;;){if(a){const r=parseFloat(o);isNaN(r)?n.push(o):n.push(Math.ceil(r*i*t)/t)}else n.push(o);if(o=s.shift(),o===void 0)return n.join("");a=!a}}function It(e,i="defs"){let t="";const s=e.indexOf("<"+i);for(;s>=0;){const n=e.indexOf(">",s),o=e.indexOf("</"+i);if(n===-1||o===-1)break;const a=e.indexOf(">",o);if(a===-1)break;t+=e.slice(n+1,o).trim(),e=e.slice(0,s).trim()+e.slice(a+1)}return{defs:t,content:e}}function Ct(e,i){return e?"<defs>"+e+"</defs>"+i:i}function Dt(e,i,t){const s=It(e);return Ct(s.defs,i+s.content+t)}const jt=e=>e==="unset"||e==="undefined"||e==="none";function Fe(e,i){const t={...R,...e},s={...Te,...i},n={left:t.left,top:t.top,width:t.width,height:t.height};let o=t.body;[t,s].forEach(I=>{const y=[],B=I.hFlip,C=I.vFlip;let $=I.rotate;B?C?$+=2:(y.push("translate("+(n.width+n.left).toString()+" "+(0-n.top).toString()+")"),y.push("scale(-1 1)"),n.top=n.left=0):C&&(y.push("translate("+(0-n.left).toString()+" "+(n.height+n.top).toString()+")"),y.push("scale(1 -1)"),n.top=n.left=0);let w;switch($<0&&($-=Math.floor($/4)*4),$=$%4,$){case 1:w=n.height/2+n.top,y.unshift("rotate(90 "+w.toString()+" "+w.toString()+")");break;case 2:y.unshift("rotate(180 "+(n.width/2+n.left).toString()+" "+(n.height/2+n.top).toString()+")");break;case 3:w=n.width/2+n.left,y.unshift("rotate(-90 "+w.toString()+" "+w.toString()+")");break}$%2===1&&(n.left!==n.top&&(w=n.left,n.left=n.top,n.top=w),n.width!==n.height&&(w=n.width,n.width=n.height,n.height=w)),y.length&&(o=Dt(o,'<g transform="'+y.join(" ")+'">',"</g>"))});const a=s.width,r=s.height,l=n.width,c=n.height;let d,f;a===null?(f=r===null?"1em":r==="auto"?c:r,d=te(f,l/c)):(d=a==="auto"?l:a,f=r===null?te(d,c/l):r==="auto"?c:r);const m={},x=(I,y)=>{jt(y)||(m[I]=y.toString())};x("width",d),x("height",f);const T=[n.left,n.top,l,c];return m.viewBox=T.join(" "),{attributes:m,viewBox:T,body:o}}function oe(e,i){let t=e.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const s in i)t+=" "+s+'="'+i[s]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+t+">"+e+"</svg>"}function Pt(e){return e.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function At(e){return"data:image/svg+xml,"+Pt(e)}function Me(e){return'url("'+At(e)+'")'}const Ot=()=>{let e;try{if(e=fetch,typeof e=="function")return e}catch{}};let H=Ot();function Et(e){H=e}function Lt(){return H}function Rt(e,i){const t=V(e);if(!t)return 0;let s;if(!t.maxURL)s=0;else{let n=0;t.resources.forEach(a=>{n=Math.max(n,a.length)});const o=i+".json?icons=";s=t.maxURL-n-t.path.length-o.length}return s}function Nt(e){return e===404}const Ft=(e,i,t)=>{const s=[],n=Rt(e,i),o="icons";let a={type:o,provider:e,prefix:i,icons:[]},r=0;return t.forEach((l,c)=>{r+=l.length+1,r>=n&&c>0&&(s.push(a),a={type:o,provider:e,prefix:i,icons:[]},r=l.length),a.icons.push(l)}),s.push(a),s};function Mt(e){if(typeof e=="string"){const i=V(e);if(i)return i.path}return"/"}const qt=(e,i,t)=>{if(!H){t("abort",424);return}let s=Mt(i.provider);switch(i.type){case"icons":{const o=i.prefix,a=i.icons.join(","),r=new URLSearchParams({icons:a});s+=o+".json?"+r.toString();break}case"custom":{const o=i.uri;s+=o.slice(0,1)==="/"?o.slice(1):o;break}default:t("abort",400);return}let n=503;H(e+s).then(o=>{const a=o.status;if(a!==200){setTimeout(()=>{t(Nt(a)?"abort":"next",a)});return}return n=501,o.json()}).then(o=>{if(typeof o!="object"||o===null){setTimeout(()=>{o===404?t("abort",o):t("next",n)});return}setTimeout(()=>{t("success",o)})}).catch(()=>{t("next",n)})},zt={prepare:Ft,send:qt};function Ut(e,i,t){S(t||"",i).loadIcons=e}function Jt(e,i,t){S(t||"",i).loadIcon=e}const Y="data-style";let qe="";function Ht(e){qe=e}function ye(e,i){let t=Array.from(e.childNodes).find(s=>s.hasAttribute&&s.hasAttribute(Y));t||(t=document.createElement("style"),t.setAttribute(Y,Y),e.appendChild(t)),t.textContent=":host{display:inline-block;vertical-align:"+(i?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+qe}function ze(){pe("",zt),Ae(!0);let e;try{e=window}catch{}if(e){if(e.IconifyPreload!==void 0){const t=e.IconifyPreload,s="Invalid IconifyPreload syntax.";typeof t=="object"&&t!==null&&(t instanceof Array?t:[t]).forEach(n=>{try{(typeof n!="object"||n===null||n instanceof Array||typeof n.icons!="object"||typeof n.prefix!="string"||!he(n))&&console.error(s)}catch{console.error(s)}})}if(e.IconifyProviders!==void 0){const t=e.IconifyProviders;if(typeof t=="object"&&t!==null)for(const s in t){const n="IconifyProviders["+s+"] is invalid.";try{const o=t[s];if(typeof o!="object"||!o||o.resources===void 0)continue;fe(s,o)||console.error(n)}catch{console.error(n)}}}}return{iconLoaded:lt,getIcon:ct,listIcons:at,addIcon:Oe,addCollection:he,calculateSize:te,buildIcon:Fe,iconToHTML:oe,svgToURL:Me,loadIcons:re,loadIcon:kt,addAPIProvider:fe,setCustomIconLoader:Jt,setCustomIconsLoader:Ut,appendCustomStyle:Ht,_api:{getAPIConfig:V,setAPIModule:pe,sendAPIQuery:Re,setFetch:Et,getFetch:Lt,listAPIProviders:gt}}}const ie={"background-color":"currentColor"},Ue={"background-color":"transparent"},xe={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},we={"-webkit-mask":ie,mask:ie,background:Ue};for(const e in we){const i=we[e];for(const t in xe)i[e+"-"+t]=xe[t]}function ke(e){return e?e+(e.match(/^[-0-9.]+$/)?"px":""):"inherit"}function Qt(e,i,t){const s=document.createElement("span");let n=e.body;n.indexOf("<a")!==-1&&(n+="<!-- "+Date.now()+" -->");const o=e.attributes,a=oe(n,{...o,width:i.width+"",height:i.height+""}),r=Me(a),l=s.style,c={"--svg":r,width:ke(o.width),height:ke(o.height),...t?ie:Ue};for(const d in c)l.setProperty(d,c[d]);return s}let O;function Vt(){try{O=window.trustedTypes.createPolicy("iconify",{createHTML:e=>e})}catch{O=null}}function Bt(e){return O===void 0&&Vt(),O?O.createHTML(e):e}function Gt(e){const i=document.createElement("span"),t=e.attributes;let s="";t.width||(s="width: inherit;"),t.height||(s+="height: inherit;"),s&&(t.style=s);const n=oe(e.body,t);return i.innerHTML=Bt(n),i.firstChild}function ne(e){return Array.from(e.childNodes).find(i=>{const t=i.tagName&&i.tagName.toUpperCase();return t==="SPAN"||t==="SVG"})}function $e(e,i){const t=i.icon.data,s=i.customisations,n=Fe(t,s);s.preserveAspectRatio&&(n.attributes.preserveAspectRatio=s.preserveAspectRatio);const o=i.renderedMode;let a;o==="svg"?a=Gt(n):a=Qt(n,{...R,...t},o==="mask");const r=ne(e);r?a.tagName==="SPAN"&&r.tagName===a.tagName?r.setAttribute("style",a.getAttribute("style")):e.replaceChild(a,r):e.appendChild(a)}function _e(e,i,t){const s=t&&(t.rendered?t:t.lastRender);return{rendered:!1,inline:i,icon:e,lastRender:s}}function Kt(e="iconify-icon"){let i,t;try{i=window.customElements,t=window.HTMLElement}catch{return}if(!i||!t)return;const s=i.get(e);if(s)return s;const n=["icon","mode","inline","noobserver","width","height","rotate","flip"],o=class extends t{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const r=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");ye(r,l),this._state=_e({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return n.slice(0)}attributeChangedCallback(r){switch(r){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,ye(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const r=this.getAttribute("icon");if(r&&r.slice(0,1)==="{")try{return JSON.parse(r)}catch{}return r}set icon(r){typeof r=="object"&&(r=JSON.stringify(r)),this.setAttribute("icon",r)}get inline(){return this.hasAttribute("inline")}set inline(r){r?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(r){r?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const r=this._state;if(r.rendered){const l=this._shadowRoot;if(r.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}$e(l,r)}}get status(){const r=this._state;return r.rendered?"rendered":r.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const r=this._state,l=this.getAttribute("icon");if(l!==r.icon.value){this._iconChanged(l);return}if(!r.rendered||!this._visible)return;const c=this.getAttribute("mode"),d=de(this);(r.attrMode!==c||et(r.customisations,d)||!ne(this._shadowRoot))&&this._renderIcon(r.icon,d,c)}_iconChanged(r){const l=$t(r,(c,d,f)=>{const m=this._state;if(m.rendered||this.getAttribute("icon")!==c)return;const x={value:c,name:d,data:f};x.data?this._gotIconData(x):m.icon=x});l.data?this._gotIconData(l):this._state=_e(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const r=ne(this._shadowRoot);r&&this._shadowRoot.removeChild(r);return}this._queueCheck()}_gotIconData(r){this._checkQueued=!1,this._renderIcon(r,de(this),this.getAttribute("mode"))}_renderIcon(r,l,c){const d=_t(r.data.body,c),f=this._state.inline;$e(this._shadowRoot,this._state={rendered:!0,icon:r,inline:f,customisations:l,attrMode:c,renderedMode:d})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(r=>{const l=r.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};n.forEach(r=>{r in o.prototype||Object.defineProperty(o.prototype,r,{get:function(){return this.getAttribute(r)},set:function(l){l!==null?this.setAttribute(r,l):this.removeAttribute(r)}})});const a=ze();for(const r in a)o[r]=o.prototype[r]=a[r];return i.define(e,o),o}const Wt=Kt()||ze(),{iconLoaded:ii,getIcon:ni,listIcons:si,addIcon:ri,addCollection:oi,calculateSize:ai,buildIcon:li,iconToHTML:ci,svgToURL:di,loadIcons:ui,loadIcon:hi,setCustomIconLoader:pi,setCustomIconsLoader:fi,addAPIProvider:gi,_api:mi}=Wt;async function v(e,i){const t=await fetch(e,{...i,headers:{...i?.body?{"content-type":"application/json"}:{},...i?.headers}});if(!t.ok){const s=await t.json().catch(()=>({error:t.statusText}));throw new Error(s.error||t.statusText)}return t.status===204?void 0:t.json()}var Yt=Object.defineProperty,Xt=Object.getOwnPropertyDescriptor,g=(e,i,t,s)=>{for(var n=s>1?void 0:s?Xt(i,t):i,o=e.length-1,a;o>=0;o--)(a=e[o])&&(n=(s?a(i,t,n):a(n))||n);return s&&n&&Yt(i,t,n),n};const z=["system","dark","bright"],Zt={system:Be,dark:Ve,bright:Ge};function ei(){const e=localStorage.getItem("upgrid-theme");return z.includes(e)?e:"system"}let p=class extends He{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.secrets=[],this.joinTokens=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.joinCommand="",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection="overview",this.copied=!1,this.theme=ei(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),this.refresh(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),this.events?.close(),super.disconnectedCallback()}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=z[(z.indexOf(this.theme)+1)%z.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}async refresh(){try{[this.targets,this.channels,this.alerts,this.secrets,this.cluster,this.joinTokens]=await Promise.all([v("/api/v1/targets"),v("/api/v1/channels"),v("/api/v1/alerts"),v("/api/v1/secrets"),v("/api/v1/cluster"),v("/api/v1/join-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),t=i?.querySelector("form");t&&(this.detailInitialState=this.detailFormState(t)),i?.showModal()})}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&(this.detailDirty=!1,this.detailInitialState="",this.selected=void 0))}navigate(e,i){e.preventDefault(),this.activeSection=i,this.updateComplete.then(()=>this.renderRoot.querySelector(`#${i}`)?.scrollIntoView({behavior:"smooth",block:"start"}))}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const i=e.currentTarget,t=i.form?.elements.namedItem("max_redirects");t&&(t.disabled=!i.checked),i.form&&this.compareDetailForm(i.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}async createTarget(e){e.preventDefault();const i=e.currentTarget,t=new FormData(i),s={name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:[]};this.saving=!0;try{await v("/api/v1/targets",{method:"POST",body:JSON.stringify(s)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=e.currentTarget,t=new FormData(i),s=String(t.get("statuses")).split(",").map(a=>{const[r,l]=a.trim().split("-").map(Number);return{start:r,end:l||r}}),n=t.get("follow_redirects")==="on",o={name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:s,follow_redirects:n,max_redirects:n?Number(t.get("max_redirects")):0,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([a,r])=>[a,r.kind==="literal"?r.value:{secret_id:r.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(t.get("body_contains"))||null,skip_tls_verification:t.get("skip_tls_verification")==="on",notification_channel_ids:this.selected.notification_channel_ids};this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(o)}),this.closeDetailDialog(),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,t=new FormData(i);this.saving=!0;try{await v("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:t.get("name"),value:t.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const i=e.currentTarget,t=new FormData(i),s=this.channelKind==="telegram"?{type:"telegram",name:t.get("name"),bot_token:t.get("bot_token"),chat_id:t.get("chat_id")}:{type:"webhook",name:t.get("name"),url:t.get("url"),headers:{}};this.saving=!0;try{await v("/api/v1/channels",{method:"POST",body:JSON.stringify(s)}),i.reset(),this.channelKind="webhook",this.closeDialog("channel-dialog"),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async createJoinLink(){this.saving=!0;try{const e=await v("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:600})});this.joinCommand=`upgrid --join '${e.url}'`,this.copied=!1,await this.refresh(),this.showDialog("join-dialog")}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await v(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const i=document.createElement("textarea");i.value=this.joinCommand,i.style.position="fixed",i.style.opacity="0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const t=new Set(this.selectedIds);i?t.add(e):t.delete(e),this.selectedIds=t}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>v(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>v(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,i,t){if(window.confirm(`Delete ${t}?`))try{await v(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}}render(){const e=this.targets.filter(n=>n.availability==="up").length,i=this.targets.filter(n=>n.availability==="down").length,t=this.alerts.filter(n=>n.delivery==="pending").length,s=this.targets.filter(n=>`${n.name} ${n.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(n=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?n.paused:n.availability===this.statusFilter).sort((n,o)=>this.sort==="status"&&n.availability.localeCompare(o.availability)||n.name.localeCompare(o.name));return u`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"live":"connecting"}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${["overview","alerts","cluster"].map(n=>u`<a class=${this.activeSection===n?"active":""} href=${`#${n}`} @click=${o=>this.navigate(o,n)}>${n[0].toUpperCase()}${n.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${Zt[this.theme]} aria-hidden="true"></iconify-icon></button>
            <button class="button secondary" @click=${this.createJoinLink} ?disabled=${this.saving}>Add node</button>
          </div>
        </header>
        ${this.error?u`<div class="notice" role="alert">${this.error}</div>`:_}
        ${this.activeSection==="overview"?this.renderOverview(s,e,i,t):this.activeSection==="alerts"?this.renderAlertsPage():this.renderClusterPage()}
      </main>
      <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="add-target-title">Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
        <form @submit=${this.createTarget}>
          <label>Name<input name="name" placeholder="Production API" required /></label>
          <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
          <div class="row">
            <label>Method<input name="method" value="GET" required /></label>
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          </div>
          <div class="row">
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
            <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
          </div>
          <div class="dialog-actions">
            <button class="button secondary" type="button" @click=${this.closeTargetDialog}>Cancel</button>
            <button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create target"}</button>
          </div>
        </form>
      </dialog>
      ${this.selected?this.renderDetail(this.selected):_}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="secret-title">Add secret</h2><p>The plaintext is encrypted before replication and never returned.</p></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">Add channel</h2><p>Send transitions through Telegram or a generic webhook.</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" @change=${n=>this.channelKind=n.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind==="webhook"?u`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:u`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("channel-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join a node</h2><p>This reusable command contains Cluster credentials. Revoke it when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderOverview(e,i,t,s){const n=this.targets.filter(r=>this.selectedIds.has(r.id)),o=n.some(r=>!r.paused),a=n.some(r=>r.paused);return u`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="summary" aria-label="Target summary">
        <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
        <div class="metric"><span>Up</span><strong>${i}</strong></div>
        <div class="metric"><span>Down</span><strong>${t}</strong></div>
        <div class="metric"><span>Pending alerts</span><strong>${s}</strong></div>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${r=>this.search=r.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${r=>this.statusFilter=r.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${r=>this.sort=r.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?u`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${ce} aria-hidden="true"></iconify-icon></button>${o?u`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${ae} aria-hidden="true"></iconify-icon></button>`:_}${a?u`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${le} aria-hidden="true"></iconify-icon></button>`:_}<button class="button danger" @click=${this.bulkDelete}>Delete selected</button></div></div>`:_}
        ${e.length?e.map(r=>this.renderTarget(r)):u`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
      <section class="resources" aria-label="Notification configuration">
        <section class="panel">
          <div class="panel-head"><h2>Notification channels</h2><button class="button secondary" @click=${()=>this.showDialog("channel-dialog")}>Add channel</button></div>
          ${this.channels.length?this.channels.map(r=>u`<div class="resource"><div><strong>${r.name}</strong><code>${r.destination}</code></div><div class="actions"><span class="badge">${r.kind}</span><button class="button danger" aria-label=${`Delete channel ${r.name}`} @click=${()=>this.deleteResource("channels",r.id,r.name)}>Delete</button></div></div>`):u`<div class="empty">No notification channels.</div>`}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length?this.secrets.map(r=>u`<div class="resource"><div><strong>${r.name}</strong><code>${r.id}</code></div><div class="actions"><span class="badge">write-only</span><button class="button danger" aria-label=${`Delete secret ${r.name}`} @click=${()=>this.deleteResource("secrets",r.id,r.name)}>Delete</button></div></div>`):u`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
    `}renderAlertsPage(){return u`
      <section class="heading" id="alerts">
        <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      </section>
      <section class="panel" aria-label="Alert history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${this.alerts.length} events</span></div>
        ${this.alerts.length?this.alerts.map(e=>u`<div class="resource"><div><strong>${e.target_name}</strong><code>${new Date(e.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${e.kind} · ${e.delivery}</span></div>`):u`<div class="empty">No availability transitions.</div>`}
      </section>
    `}renderClusterPage(){return u`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <button class="button" @click=${this.createJoinLink}>Add node</button>
      </section>
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><h2>Nodes</h2><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(e=>u`<div class="resource"><div><strong>${e.raft_url}</strong><code>${e.id}</code></div><div class="actions">${e.local?u`<span class="badge">This node</span>`:_}${e.leader?u`<span class="badge">Leader</span>`:_}</div></div>`)}
        ${this.cluster?.members.length?_:u`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens" style="margin-top: 18px">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(e=>u`
              <div class="resource">
                <div><strong>${e.id.slice(0,12)}…</strong><code>Expires ${new Date(e.expires_at_ms).toLocaleString()}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${e.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(e)}>Revoke</button>
              </div>
            `):u`<div class="empty">No Join Tokens.</div>`}
      </section>
    `}renderTarget(e){const i=e.latest_evaluation,t=e.history.slice(0,16).reverse(),s=Math.max(1,...t.map(n=>n.latency_ms));return u`
      <div class="target-wrap">
        <input class="select-target" type="checkbox" aria-label=${`Select ${e.name}`} .checked=${this.selectedIds.has(e.id)} @change=${n=>this.toggleSelected(e.id,n.target.checked)} />
        <button class="target" aria-label=${e.name} @click=${()=>this.openTarget(e)}>
          <i class="state ${e.paused?"paused":e.availability}" aria-label=${e.paused?"paused":e.availability}></i>
          <div>
            <h3>${e.name}</h3>
            <div class="meta">${e.paused?"Paused · ":""}${e.method} · ${e.url} · every ${e.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${t.length?u`<div class="mini-chart" aria-hidden="true">${t.map(n=>u`<i class="mini-bar ${n.succeeded?"up":"down"}" style=${`height: ${Math.max(12,n.latency_ms/s*100)}%`}></i>`)}</div>`:_}
            <div class="latency">
              <strong>${i?`${i.latency_ms} ms`:"—"}</strong>
              <span>${i?i.status_code??"network error":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}renderDetail(e){const i=e.accepted_statuses.map(a=>a.start===a.end?a.start:`${a.start}-${a.end}`).join(","),t=e.history.slice(0,30).reverse(),s=Math.max(1,...t.map(a=>a.latency_ms)),n=a=>new Date(a).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),o=a=>a>=1e3?`${(a/1e3).toFixed(a>=1e4?0:1)} s`:`${Math.round(a)} ms`;return u`
      <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head">
          <h2 id="target-detail-title">Target details</h2>
          <button class="button secondary icon-button dialog-close" type="button" aria-label="Close target details" title="Close" @click=${this.closeDetailDialog}><iconify-icon .icon=${ce} aria-hidden="true"></iconify-icon></button>
        </div>
        <form @submit=${this.updateTarget} @input=${this.updateDetailDirty}>
          <label>Name<input name="name" .value=${e.name} required /></label>
          <label>URL<input name="url" type="url" .value=${e.url} required /></label>
          <div class="row">
            <label>Method<input name="method" .value=${e.method} required /></label>
            <label>Expected statuses<input name="statuses" .value=${i} required /></label>
          </div>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(e.interval_seconds)} required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(e.timeout_seconds)} required /></label>
          </div>
          <div class="row">
            <label>Failures before Down<input name="failures" type="number" min="1" .value=${String(e.failure_threshold)} required /></label>
            <label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(e.max_redirects)} ?disabled=${!e.follow_redirects} required /></label>
          </div>
          <label>Body must contain<input name="body_contains" .value=${e.body_contains??""} /></label>
          <div class="row">
            <label class="check"><input name="follow_redirects" type="checkbox" .checked=${e.follow_redirects} @change=${this.toggleMaxRedirects} />Follow redirects</label>
            <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${e.skip_tls_verification} />Skip TLS verification</label>
          </div>
          <div class="dialog-actions">
            <div class="danger-actions">
              <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${this.deleteTarget}><iconify-icon .icon=${Ke} aria-hidden="true"></iconify-icon></button>
              <button class=${`button ${e.paused?"success":"warning"} icon-button`} type="button" aria-label=${e.paused?"Resume evaluations":"Pause evaluations"} title=${e.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>this.setPaused(!e.paused)}><iconify-icon .icon=${e.paused?le:ae} aria-hidden="true"></iconify-icon></button>
            </div>
            <button class="button" type="submit" aria-busy=${this.saving?"true":"false"} ?disabled=${this.saving||!this.detailDirty}>Save changes</button>
          </div>
        </form>
        <section class="history">
          <div class="history-head"><h3>Evaluation history</h3>${t.length?u`<span class="meta">Latest ${t.length}</span>`:_}</div>
          ${t.length?u`
                <div class="chart-plot">
                  <div class="chart-scale" aria-hidden="true"><span>${o(s)}</span><span>${o(s/2)}</span><span>0 ms</span></div>
                  <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${o(s)}`}>
                    ${t.map(a=>{const r=a.succeeded?"Passed":"Failed",l=a.status_code===null?"network error":`HTTP ${a.status_code}`,c=`${r} at ${new Date(a.recorded_at_ms).toLocaleString()}: ${a.latency_ms} ms, ${l}`;return u`<span class="history-bar ${a.succeeded?"up":"down"}" role="listitem" aria-label=${c} title=${c} style=${`height: ${Math.max(8,a.latency_ms/s*100)}%`}></span>`})}
                  </div>
                </div>
                <div class="chart-axis"><span>${n(t[0].recorded_at_ms)}</span><span>${n(t.at(-1).recorded_at_ms)}</span></div>
                <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
              `:u`<p class="meta">No evaluations recorded yet.</p>`}
        </section>
      </dialog>
    `}};p.styles=Je`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      --page-background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      --brand-shadow: #40d89035;
      --nav-bg: #0d1210aa;
      --active-bg: #202b27;
      --button-border: #3e765a;
      --button-bg: #1c4a35;
      --button-text: #e8fff2;
      --button-hover-border: #62b988;
      --panel-surface: #111715dc;
      --panel-shadow: #0002;
      --divider: #202925;
      --badge-border: #3c554a;
      --badge-text: #a7c3b7;
      --row-hover: #17201c;
      --notice-border: #7b3937;
      --notice-bg: #391b1a;
      --notice-text: #ffb3af;
      --bulk-bg: #16221d;
      --dialog-shadow: #000b;
      --backdrop: #040706cc;
      --input-bg: #0c110f;
      --focus: #4b936c;
      --danger-text: #ff9b97;
      --danger-border: #633b39;
      --warning-bg: #594315;
      --warning-text: #ffd778;
      --warning-border: #9c7625;
      --join-bg: #0b110e;
      display: block;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { max-width: 1200px; margin: auto; padding: 28px 24px 72px; }
    header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    .brand-line { display: flex; align-items: center; gap: 12px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); transition: background-color 160ms ease, box-shadow 160ms ease; }
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 36px; height: 36px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
    .resources { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 15px; height: 15px; accent-color: var(--green); }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .state { width: 10px; height: 10px; border-radius: 50%; background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open] { opacity: 1; transform: translateY(0) scale(1); }
    dialog::backdrop { background: var(--backdrop); backdrop-filter: blur(5px); opacity: 0; transition: opacity 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open]::backdrop { opacity: 1; }
    @starting-style {
      dialog[open] { opacity: 0; transform: translateY(8px) scale(.985); }
      dialog[open]::backdrop { opacity: 0; }
    }
    .dialog-head { position: relative; padding: 20px 58px 15px 22px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; }
    .check { display: flex; align-items: center; gap: 8px; }
    .check input { width: auto; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .chart-plot { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 7px; }
    .chart-scale { display: flex; height: 140px; flex-direction: column; justify-content: space-between; padding: 1px 0 7px; color: var(--muted); font-size: 9px; text-align: right; }
    .history-chart { display: flex; height: 140px; align-items: flex-end; gap: 3px; padding: 14px 10px 8px; border: 1px solid var(--line); border-radius: 10px; background: var(--input-bg); }
    .history-bar { flex: 1; min-width: 3px; max-width: 16px; border-radius: 3px 3px 1px 1px; opacity: .82; transform-origin: bottom; transition: opacity 120ms ease, transform 120ms ease; }
    .history-bar:hover { opacity: 1; transform: scaleX(1.15); }
    .history-bar.up { background: var(--green); }
    .history-bar.down { background: var(--red); }
    .chart-axis { justify-content: space-between; margin: 5px 0 0 45px; color: var(--muted); font-size: 10px; }
    .chart-legend { justify-content: flex-end; gap: 12px; margin-top: 9px; color: var(--muted); font-size: 10px; }
    .chart-legend span { gap: 5px; }
    .chart-legend i { width: 7px; height: 7px; border-radius: 2px; }
    .chart-legend .up { background: var(--green); }
    .chart-legend .down { background: var(--red); }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    :host([data-theme="bright"]) {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --panel-2: #eef5f1;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #c53434;
        --amber: #9a6700;
        --page-background:
          radial-gradient(circle at 12% -5%, #d9f2e4 0, transparent 32%),
          linear-gradient(145deg, #fbfdfc 0%, #f3f8f5 55%, #edf5f1 100%);
        --brand-shadow: #159e5240;
        --nav-bg: #ffffffcc;
        --active-bg: #e4efe9;
        --button-border: #16764b;
        --button-bg: #087a49;
        --button-text: #ffffff;
        --button-hover-border: #075f3a;
        --panel-surface: #ffffffeb;
        --panel-shadow: #2745381a;
        --divider: #e3ebe7;
        --badge-border: #a6beb2;
        --badge-text: #426356;
        --row-hover: #e9f4ee;
        --notice-border: #e2aaa6;
        --notice-bg: #fff0ef;
        --notice-text: #9f2922;
        --bulk-bg: #e8f4ed;
        --dialog-shadow: #233b3050;
        --backdrop: #17251f66;
        --input-bg: #ffffff;
        --focus: #168655;
        --danger-text: #b42318;
        --danger-border: #dda29d;
        --warning-bg: #fff1bd;
        --warning-text: #805b00;
        --warning-border: #d4aa36;
        --join-bg: #eef8f2;
    }
    @media (prefers-reduced-motion: reduce) {
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      nav { display: none; }
      .summary { grid-template-columns: 1fr 1fr; }
      .resources { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .target-side { grid-column: 2; justify-self: start; }
      .latency { text-align: left; }
    }
  `;g([b()],p.prototype,"targets",2);g([b()],p.prototype,"channels",2);g([b()],p.prototype,"alerts",2);g([b()],p.prototype,"secrets",2);g([b()],p.prototype,"cluster",2);g([b()],p.prototype,"joinTokens",2);g([b()],p.prototype,"error",2);g([b()],p.prototype,"live",2);g([b()],p.prototype,"saving",2);g([b()],p.prototype,"selected",2);g([b()],p.prototype,"channelKind",2);g([b()],p.prototype,"joinCommand",2);g([b()],p.prototype,"search",2);g([b()],p.prototype,"statusFilter",2);g([b()],p.prototype,"sort",2);g([b()],p.prototype,"selectedIds",2);g([b()],p.prototype,"activeSection",2);g([b()],p.prototype,"copied",2);g([b()],p.prototype,"theme",2);g([b()],p.prototype,"detailDirty",2);p=g([Qe("upgrid-app")],p);
