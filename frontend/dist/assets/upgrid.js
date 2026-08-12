(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const n of document.querySelectorAll('link[rel="modulepreload"]'))s(n);new MutationObserver(n=>{for(const r of n)if(r.type==="childList")for(const a of r.addedNodes)a.tagName==="LINK"&&a.rel==="modulepreload"&&s(a)}).observe(document,{childList:!0,subtree:!0});function i(n){const r={};return n.integrity&&(r.integrity=n.integrity),n.referrerPolicy&&(r.referrerPolicy=n.referrerPolicy),n.crossOrigin==="use-credentials"?r.credentials="include":n.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function s(n){if(n.ep)return;n.ep=!0;const r=i(n);fetch(n.href,r)}})();const ne=globalThis,Me=ne.ShadowRoot&&(ne.ShadyCSS===void 0||ne.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Le=Symbol(),Qe=new WeakMap;let St=class{constructor(e,i,s){if(this._$cssResult$=!0,s!==Le)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=i}get styleSheet(){let e=this.o;const i=this.t;if(Me&&e===void 0){const s=i!==void 0&&i.length===1;s&&(e=Qe.get(i)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),s&&Qe.set(i,e))}return e}toString(){return this.cssText}};const ti=t=>new St(typeof t=="string"?t:t+"",void 0,Le),fe=(t,...e)=>{const i=t.length===1?t[0]:e.reduce((s,n,r)=>s+(a=>{if(a._$cssResult$===!0)return a.cssText;if(typeof a=="number")return a;throw Error("Value passed to 'css' function must be a 'css' function result: "+a+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(n)+t[r+1],t[0]);return new St(i,t,Le)},ii=(t,e)=>{if(Me)t.adoptedStyleSheets=e.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of e){const s=document.createElement("style"),n=ne.litNonce;n!==void 0&&s.setAttribute("nonce",n),s.textContent=i.cssText,t.appendChild(s)}},Ye=Me?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let i="";for(const s of e.cssRules)i+=s.cssText;return ti(i)})(t):t;const{is:si,defineProperty:ni,getOwnPropertyDescriptor:ai,getOwnPropertyNames:ri,getOwnPropertySymbols:oi,getPrototypeOf:li}=Object,me=globalThis,Ze=me.trustedTypes,ci=Ze?Ze.emptyScript:"",di=me.reactiveElementPolyfillSupport,G=(t,e)=>t,le={toAttribute(t,e){switch(e){case Boolean:t=t?ci:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let i=t;switch(e){case Boolean:i=t!==null;break;case Number:i=t===null?null:Number(t);break;case Object:case Array:try{i=JSON.parse(t)}catch{i=null}}return i}},Ue=(t,e)=>!si(t,e),Xe={attribute:!0,type:String,converter:le,reflect:!1,useDefault:!1,hasChanged:Ue};Symbol.metadata??=Symbol("metadata"),me.litPropertyMetadata??=new WeakMap;let q=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,i=Xe){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(e,i),!i.noAccessor){const s=Symbol(),n=this.getPropertyDescriptor(e,s,i);n!==void 0&&ni(this.prototype,e,n)}}static getPropertyDescriptor(e,i,s){const{get:n,set:r}=ai(this.prototype,e)??{get(){return this[i]},set(a){this[i]=a}};return{get:n,set(a){const o=n?.call(this);r?.call(this,a),this.requestUpdate(e,o,s)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??Xe}static _$Ei(){if(this.hasOwnProperty(G("elementProperties")))return;const e=li(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(G("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(G("properties"))){const i=this.properties,s=[...ri(i),...oi(i)];for(const n of s)this.createProperty(n,i[n])}const e=this[Symbol.metadata];if(e!==null){const i=litPropertyMetadata.get(e);if(i!==void 0)for(const[s,n]of i)this.elementProperties.set(s,n)}this._$Eh=new Map;for(const[i,s]of this.elementProperties){const n=this._$Eu(i,s);n!==void 0&&this._$Eh.set(n,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const i=[];if(Array.isArray(e)){const s=new Set(e.flat(1/0).reverse());for(const n of s)i.unshift(Ye(n))}else e!==void 0&&i.push(Ye(e));return i}static _$Eu(e,i){const s=i.attribute;return s===!1?void 0:typeof s=="string"?s:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,i=this.constructor.elementProperties;for(const s of i.keys())this.hasOwnProperty(s)&&(e.set(s,this[s]),delete this[s]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return ii(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,i,s){this._$AK(e,s)}_$ET(e,i){const s=this.constructor.elementProperties.get(e),n=this.constructor._$Eu(e,s);if(n!==void 0&&s.reflect===!0){const r=(s.converter?.toAttribute!==void 0?s.converter:le).toAttribute(i,s.type);this._$Em=e,r==null?this.removeAttribute(n):this.setAttribute(n,r),this._$Em=null}}_$AK(e,i){const s=this.constructor,n=s._$Eh.get(e);if(n!==void 0&&this._$Em!==n){const r=s.getPropertyOptions(n),a=typeof r.converter=="function"?{fromAttribute:r.converter}:r.converter?.fromAttribute!==void 0?r.converter:le;this._$Em=n;const o=a.fromAttribute(i,r.type);this[n]=o??this._$Ej?.get(n)??o,this._$Em=null}}requestUpdate(e,i,s,n=!1,r){if(e!==void 0){const a=this.constructor;if(n===!1&&(r=this[e]),s??=a.getPropertyOptions(e),!((s.hasChanged??Ue)(r,i)||s.useDefault&&s.reflect&&r===this._$Ej?.get(e)&&!this.hasAttribute(a._$Eu(e,s))))return;this.C(e,i,s)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,i,{useDefault:s,reflect:n,wrapped:r},a){s&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,a??i??this[e]),r!==!0||a!==void 0)||(this._$AL.has(e)||(this.hasUpdated||s||(i=void 0),this._$AL.set(e,i)),n===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[n,r]of this._$Ep)this[n]=r;this._$Ep=void 0}const s=this.constructor.elementProperties;if(s.size>0)for(const[n,r]of s){const{wrapped:a}=r,o=this[n];a!==!0||this._$AL.has(n)||o===void 0||this.C(n,void 0,r,o)}}let e=!1;const i=this._$AL;try{e=this.shouldUpdate(i),e?(this.willUpdate(i),this._$EO?.forEach(s=>s.hostUpdate?.()),this.update(i)):this._$EM()}catch(s){throw e=!1,this._$EM(),s}e&&this._$AE(i)}willUpdate(e){}_$AE(e){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(e){}firstUpdated(e){}};q.elementStyles=[],q.shadowRootOptions={mode:"open"},q[G("elementProperties")]=new Map,q[G("finalized")]=new Map,di?.({ReactiveElement:q}),(me.reactiveElementVersions??=[]).push("2.1.2");const qe=globalThis,et=t=>t,ce=qe.trustedTypes,tt=ce?ce.createPolicy("lit-html",{createHTML:t=>t}):void 0,Tt="$lit$",I=`lit$${Math.random().toFixed(9).slice(2)}$`,At="?"+I,pi=`<${At}>`,M=document,Q=()=>M.createComment(""),Y=t=>t===null||typeof t!="object"&&typeof t!="function",Fe=Array.isArray,ui=t=>Fe(t)||typeof t?.[Symbol.iterator]=="function",we=`[ 	
\f\r]`,B=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,it=/-->/g,st=/>/g,N=RegExp(`>|${we}(?:([^\\s"'>=/]+)(${we}*=${we}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),nt=/'/g,at=/"/g,Ct=/^(?:script|style|textarea|title)$/i,hi=t=>(e,...i)=>({_$litType$:t,strings:e,values:i}),c=hi(1),F=Symbol.for("lit-noChange"),g=Symbol.for("lit-nothing"),rt=new WeakMap,j=M.createTreeWalker(M,129);function Et(t,e){if(!Fe(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return tt!==void 0?tt.createHTML(e):e}const gi=(t,e)=>{const i=t.length-1,s=[];let n,r=e===2?"<svg>":e===3?"<math>":"",a=B;for(let o=0;o<i;o++){const l=t[o];let d,p,u=-1,v=0;for(;v<l.length&&(a.lastIndex=v,p=a.exec(l),p!==null);)v=a.lastIndex,a===B?p[1]==="!--"?a=it:p[1]!==void 0?a=st:p[2]!==void 0?(Ct.test(p[2])&&(n=RegExp("</"+p[2],"g")),a=N):p[3]!==void 0&&(a=N):a===N?p[0]===">"?(a=n??B,u=-1):p[1]===void 0?u=-2:(u=a.lastIndex-p[2].length,d=p[1],a=p[3]===void 0?N:p[3]==='"'?at:nt):a===at||a===nt?a=N:a===it||a===st?a=B:(a=N,n=void 0);const $=a===N&&t[o+1].startsWith("/>")?" ":"";r+=a===B?l+pi:u>=0?(s.push(d),l.slice(0,u)+Tt+l.slice(u)+I+$):l+I+(u===-2?o:$)}return[Et(t,r+(t[i]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),s]};class Z{constructor({strings:e,_$litType$:i},s){let n;this.parts=[];let r=0,a=0;const o=e.length-1,l=this.parts,[d,p]=gi(e,i);if(this.el=Z.createElement(d,s),j.currentNode=this.el.content,i===2||i===3){const u=this.el.content.firstChild;u.replaceWith(...u.childNodes)}for(;(n=j.nextNode())!==null&&l.length<o;){if(n.nodeType===1){if(n.hasAttributes())for(const u of n.getAttributeNames())if(u.endsWith(Tt)){const v=p[a++],$=n.getAttribute(u).split(I),w=/([.?@])?(.*)/.exec(v);l.push({type:1,index:r,name:w[2],strings:$,ctor:w[1]==="."?mi:w[1]==="?"?bi:w[1]==="@"?vi:be}),n.removeAttribute(u)}else u.startsWith(I)&&(l.push({type:6,index:r}),n.removeAttribute(u));if(Ct.test(n.tagName)){const u=n.textContent.split(I),v=u.length-1;if(v>0){n.textContent=ce?ce.emptyScript:"";for(let $=0;$<v;$++)n.append(u[$],Q()),j.nextNode(),l.push({type:2,index:++r});n.append(u[v],Q())}}}else if(n.nodeType===8)if(n.data===At)l.push({type:2,index:r});else{let u=-1;for(;(u=n.data.indexOf(I,u+1))!==-1;)l.push({type:7,index:r}),u+=I.length-1}r++}}static createElement(e,i){const s=M.createElement("template");return s.innerHTML=e,s}}function z(t,e,i=t,s){if(e===F)return e;let n=s!==void 0?i._$Co?.[s]:i._$Cl;const r=Y(e)?void 0:e._$litDirective$;return n?.constructor!==r&&(n?._$AO?.(!1),r===void 0?n=void 0:(n=new r(t),n._$AT(t,i,s)),s!==void 0?(i._$Co??=[])[s]=n:i._$Cl=n),n!==void 0&&(e=z(t,n._$AS(t,e.values),n,s)),e}class fi{constructor(e,i){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:i},parts:s}=this._$AD,n=(e?.creationScope??M).importNode(i,!0);j.currentNode=n;let r=j.nextNode(),a=0,o=0,l=s[0];for(;l!==void 0;){if(a===l.index){let d;l.type===2?d=new te(r,r.nextSibling,this,e):l.type===1?d=new l.ctor(r,l.name,l.strings,this,e):l.type===6&&(d=new yi(r,this,e)),this._$AV.push(d),l=s[++o]}a!==l?.index&&(r=j.nextNode(),a++)}return j.currentNode=M,n}p(e){let i=0;for(const s of this._$AV)s!==void 0&&(s.strings!==void 0?(s._$AI(e,s,i),i+=s.strings.length-2):s._$AI(e[i])),i++}}class te{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,i,s,n){this.type=2,this._$AH=g,this._$AN=void 0,this._$AA=e,this._$AB=i,this._$AM=s,this.options=n,this._$Cv=n?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&e?.nodeType===11&&(e=i.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,i=this){e=z(this,e,i),Y(e)?e===g||e==null||e===""?(this._$AH!==g&&this._$AR(),this._$AH=g):e!==this._$AH&&e!==F&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):ui(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==g&&Y(this._$AH)?this._$AA.nextSibling.data=e:this.T(M.createTextNode(e)),this._$AH=e}$(e){const{values:i,_$litType$:s}=e,n=typeof s=="number"?this._$AC(e):(s.el===void 0&&(s.el=Z.createElement(Et(s.h,s.h[0]),this.options)),s);if(this._$AH?._$AD===n)this._$AH.p(i);else{const r=new fi(n,this),a=r.u(this.options);r.p(i),this.T(a),this._$AH=r}}_$AC(e){let i=rt.get(e.strings);return i===void 0&&rt.set(e.strings,i=new Z(e)),i}k(e){Fe(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let s,n=0;for(const r of e)n===i.length?i.push(s=new te(this.O(Q()),this.O(Q()),this,this.options)):s=i[n],s._$AI(r),n++;n<i.length&&(this._$AR(s&&s._$AB.nextSibling,n),i.length=n)}_$AR(e=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);e!==this._$AB;){const s=et(e).nextSibling;et(e).remove(),e=s}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class be{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,i,s,n,r){this.type=1,this._$AH=g,this._$AN=void 0,this.element=e,this.name=i,this._$AM=n,this.options=r,s.length>2||s[0]!==""||s[1]!==""?(this._$AH=Array(s.length-1).fill(new String),this.strings=s):this._$AH=g}_$AI(e,i=this,s,n){const r=this.strings;let a=!1;if(r===void 0)e=z(this,e,i,0),a=!Y(e)||e!==this._$AH&&e!==F,a&&(this._$AH=e);else{const o=e;let l,d;for(e=r[0],l=0;l<r.length-1;l++)d=z(this,o[s+l],i,l),d===F&&(d=this._$AH[l]),a||=!Y(d)||d!==this._$AH[l],d===g?e=g:e!==g&&(e+=(d??"")+r[l+1]),this._$AH[l]=d}a&&!n&&this.j(e)}j(e){e===g?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class mi extends be{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===g?void 0:e}}class bi extends be{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==g)}}class vi extends be{constructor(e,i,s,n,r){super(e,i,s,n,r),this.type=5}_$AI(e,i=this){if((e=z(this,e,i,0)??g)===F)return;const s=this._$AH,n=e===g&&s!==g||e.capture!==s.capture||e.once!==s.once||e.passive!==s.passive,r=e!==g&&(s===g||n);n&&this.element.removeEventListener(this.name,this,s),r&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class yi{constructor(e,i,s){this.element=e,this.type=6,this._$AN=void 0,this._$AM=i,this.options=s}get _$AU(){return this._$AM._$AU}_$AI(e){z(this,e)}}const xi=qe.litHtmlPolyfillSupport;xi?.(Z,te),(qe.litHtmlVersions??=[]).push("3.3.3");const $i=(t,e,i)=>{const s=i?.renderBefore??e;let n=s._$litPart$;if(n===void 0){const r=i?.renderBefore??null;s._$litPart$=n=new te(e.insertBefore(Q(),r),r,void 0,i??{})}return n._$AI(t),n};const ze=globalThis;class R extends q{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=$i(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return F}}R._$litElement$=!0,R.finalized=!0,ze.litElementHydrateSupport?.({LitElement:R});const wi=ze.litElementPolyfillSupport;wi?.({LitElement:R});(ze.litElementVersions??=[]).push("4.2.2");const He=t=>(e,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const _i={attribute:!0,type:String,converter:le,reflect:!1,hasChanged:Ue},ki=(t=_i,e,i)=>{const{kind:s,metadata:n}=i;let r=globalThis.litPropertyMetadata.get(n);if(r===void 0&&globalThis.litPropertyMetadata.set(n,r=new Map),s==="setter"&&((t=Object.create(t)).wrapped=!0),r.set(i.name,t),s==="accessor"){const{name:a}=i;return{set(o){const l=e.get.call(this);e.set.call(this,o),this.requestUpdate(a,l,t,!0,o)},init(o){return o!==void 0&&this.C(a,void 0,t,o),o}}}if(s==="setter"){const{name:a}=i;return function(o){const l=this[a];e.call(this,o),this.requestUpdate(a,l,t,!0,o)}}throw Error("Unsupported decorator location: "+s)};function ve(t){return(e,i)=>typeof i=="object"?ki(t,e,i):((s,n,r)=>{const a=n.hasOwnProperty(r);return n.constructor.createProperty(r,s),a?Object.getOwnPropertyDescriptor(n,r):void 0})(t,e,i)}function f(t){return ve({...t,state:!0,attribute:!1})}const Pt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},Dt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},de={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},It={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const Ot=Object.freeze({left:0,top:0,width:16,height:16}),pe=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),ie=Object.freeze({...Ot,...pe}),Ce=Object.freeze({...ie,body:"",hidden:!1}),Si=Object.freeze({width:null,height:null}),Nt=Object.freeze({...Si,...pe});function Ti(t,e=0){const i=t.replace(/^-?[0-9.]*/,"");function s(n){for(;n<0;)n+=4;return n%4}if(i===""){const n=parseInt(t);return isNaN(n)?0:s(n)}else if(i!==t){let n=0;switch(i){case"%":n=25;break;case"deg":n=90}if(n){let r=parseFloat(t.slice(0,t.length-i.length));return isNaN(r)?0:(r=r/n,r%1===0?s(r):0)}}return e}const Ai=/[\s,]+/;function Ci(t,e){e.split(Ai).forEach(i=>{switch(i.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const jt={...Nt,preserveAspectRatio:""};function ot(t){const e={...jt},i=(s,n)=>t.getAttribute(s)||n;return e.width=i("width",null),e.height=i("height",null),e.rotate=Ti(i("rotate","")),Ci(e,i("flip","")),e.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),e}function Ei(t,e){for(const i in jt)if(t[i]!==e[i])return!0;return!1}const Rt=/^[a-z0-9]+(-[a-z0-9]+)*$/,se=(t,e,i,s="")=>{const n=t.split(":");if(t.slice(0,1)==="@"){if(n.length<2||n.length>3)return null;s=n.shift().slice(1)}if(n.length>3||!n.length)return null;if(n.length>1){const o=n.pop(),l=n.pop(),d={provider:n.length>0?n[0]:s,prefix:l,name:o};return e&&!ae(d)?null:d}const r=n[0],a=r.split("-");if(a.length>1){const o={provider:s,prefix:a.shift(),name:a.join("-")};return e&&!ae(o)?null:o}if(i&&s===""){const o={provider:s,prefix:"",name:r};return e&&!ae(o,i)?null:o}return null},ae=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function Pi(t,e){const i=t.icons,s=t.aliases||Object.create(null),n=Object.create(null);function r(a){if(i[a])return n[a]=[];if(!(a in n)){n[a]=null;const o=s[a]&&s[a].parent,l=o&&r(o);l&&(n[a]=[o].concat(l))}return n[a]}return Object.keys(i).concat(Object.keys(s)).forEach(r),n}function Di(t,e){const i={};!t.hFlip!=!e.hFlip&&(i.hFlip=!0),!t.vFlip!=!e.vFlip&&(i.vFlip=!0);const s=((t.rotate||0)+(e.rotate||0))%4;return s&&(i.rotate=s),i}function lt(t,e){const i=Di(t,e);for(const s in Ce)s in pe?s in t&&!(s in i)&&(i[s]=pe[s]):s in e?i[s]=e[s]:s in t&&(i[s]=t[s]);return i}function Ii(t,e,i){const s=t.icons,n=t.aliases||Object.create(null);let r={};function a(o){r=lt(s[o]||n[o],r)}return a(e),i.forEach(a),lt(t,r)}function Mt(t,e){const i=[];if(typeof t!="object"||typeof t.icons!="object")return i;t.not_found instanceof Array&&t.not_found.forEach(n=>{e(n,null),i.push(n)});const s=Pi(t);for(const n in s){const r=s[n];r&&(e(n,Ii(t,n,r)),i.push(n))}return i}const Oi={provider:"",aliases:{},not_found:{},...Ot};function _e(t,e){for(const i in e)if(i in t&&typeof t[i]!=typeof e[i])return!1;return!0}function Lt(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!_e(t,Oi))return null;const i=e.icons;for(const n in i){const r=i[n];if(!n||typeof r.body!="string"||!_e(r,Ce))return null}const s=e.aliases||Object.create(null);for(const n in s){const r=s[n],a=r.parent;if(!n||typeof a!="string"||!i[a]&&!s[a]||!_e(r,Ce))return null}return e}const ue=Object.create(null);function Ni(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function P(t,e){const i=ue[t]||(ue[t]=Object.create(null));return i[e]||(i[e]=Ni(t,e))}function Ut(t,e){return Lt(e)?Mt(e,(i,s)=>{s?t.icons[i]=s:t.missing.add(i)}):[]}function ji(t,e,i){try{if(typeof i.body=="string")return t.icons[e]={...i},!0}catch{}return!1}function Ri(t,e){let i=[];return(typeof t=="string"?[t]:Object.keys(ue)).forEach(s=>{(typeof s=="string"&&typeof e=="string"?[e]:Object.keys(ue[s]||{})).forEach(n=>{const r=P(s,n);i=i.concat(Object.keys(r.icons).map(a=>(s!==""?"@"+s+":":"")+n+":"+a))})}),i}let X=!1;function qt(t){return typeof t=="boolean"&&(X=t),X}function ee(t){const e=typeof t=="string"?se(t,!0,X):t;if(e){const i=P(e.provider,e.prefix),s=e.name;return i.icons[s]||(i.missing.has(s)?null:void 0)}}function Ft(t,e){const i=se(t,!0,X);if(!i)return!1;const s=P(i.provider,i.prefix);return e?ji(s,i.name,e):(s.missing.add(i.name),!0)}function ct(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),X&&!e&&!t.prefix){let s=!1;return Lt(t)&&(t.prefix="",Mt(t,(n,r)=>{Ft(n,r)&&(s=!0)})),s}const i=t.prefix;return ae({prefix:i,name:"a"})?!!Ut(P(e,i),t):!1}function Mi(t){return!!ee(t)}function Li(t){const e=ee(t);return e&&{...ie,...e}}function zt(t,e){t.forEach(i=>{const s=i.loaderCallbacks;s&&(i.loaderCallbacks=s.filter(n=>n.id!==e))})}function Ui(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let i=!1;const s=t.provider,n=t.prefix;e.forEach(r=>{const a=r.icons,o=a.pending.length;a.pending=a.pending.filter(l=>{if(l.prefix!==n)return!0;const d=l.name;if(t.icons[d])a.loaded.push({provider:s,prefix:n,name:d});else if(t.missing.has(d))a.missing.push({provider:s,prefix:n,name:d});else return i=!0,!0;return!1}),a.pending.length!==o&&(i||zt([t],r.id),r.callback(a.loaded.slice(0),a.missing.slice(0),a.pending.slice(0),r.abort))})}))}let qi=0;function Fi(t,e,i){const s=qi++,n=zt.bind(null,i,s);if(!e.pending.length)return n;const r={id:s,icons:e,callback:t,abort:n};return i.forEach(a=>{(a.loaderCallbacks||(a.loaderCallbacks=[])).push(r)}),n}function zi(t){const e={loaded:[],missing:[],pending:[]},i=Object.create(null);t.sort((n,r)=>n.provider!==r.provider?n.provider.localeCompare(r.provider):n.prefix!==r.prefix?n.prefix.localeCompare(r.prefix):n.name.localeCompare(r.name));let s={provider:"",prefix:"",name:""};return t.forEach(n=>{if(s.name===n.name&&s.prefix===n.prefix&&s.provider===n.provider)return;s=n;const r=n.provider,a=n.prefix,o=n.name,l=i[r]||(i[r]=Object.create(null)),d=l[a]||(l[a]=P(r,a));let p;o in d.icons?p=e.loaded:a===""||d.missing.has(o)?p=e.missing:p=e.pending;const u={provider:r,prefix:a,name:o};p.push(u)}),e}const Ee=Object.create(null);function dt(t,e){Ee[t]=e}function Pe(t){return Ee[t]||Ee[""]}function Hi(t,e=!0,i=!1){const s=[];return t.forEach(n=>{const r=typeof n=="string"?se(n,e,i):n;r&&s.push(r)}),s}function Je(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const ye=Object.create(null),K=["https://api.simplesvg.com","https://api.unisvg.com"],re=[];for(;K.length>0;)K.length===1||Math.random()>.5?re.push(K.shift()):re.push(K.pop());ye[""]=Je({resources:["https://api.iconify.design"].concat(re)});function pt(t,e){const i=Je(e);return i===null?!1:(ye[t]=i,!0)}function xe(t){return ye[t]}function Ji(){return Object.keys(ye)}const Bi={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Ki(t,e,i,s){const n=t.resources.length,r=t.random?Math.floor(Math.random()*n):t.index;let a;if(t.random){let x=t.resources.slice(0);for(a=[];x.length>1;){const h=Math.floor(Math.random()*x.length);a.push(x[h]),x=x.slice(0,h).concat(x.slice(h+1))}a=a.concat(x)}else a=t.resources.slice(r).concat(t.resources.slice(0,r));const o=Date.now();let l="pending",d=0,p,u=null,v=[],$=[];typeof s=="function"&&$.push(s);function w(){u&&(clearTimeout(u),u=null)}function S(){l==="pending"&&(l="aborted"),w(),v.forEach(x=>{x.status==="pending"&&(x.status="aborted")}),v=[]}function _(x,h){h&&($=[]),typeof x=="function"&&$.push(x)}function J(){return{startTime:o,payload:e,status:l,queriesSent:d,queriesPending:v.length,subscribe:_,abort:S}}function C(){l="failed",$.forEach(x=>{x(void 0,p)})}function T(){v.forEach(x=>{x.status==="pending"&&(x.status="aborted")}),v=[]}function k(x,h,A){const U=h!=="success";switch(v=v.filter(E=>E!==x),l){case"pending":break;case"failed":if(U||!t.dataAfterTimeout)return;break;default:return}if(h==="abort"){p=A,C();return}if(U){p=A,v.length||(a.length?D():C());return}if(w(),T(),!t.random){const E=t.resources.indexOf(x.resource);E!==-1&&E!==t.index&&(t.index=E)}l="completed",$.forEach(E=>{E(A)})}function D(){if(l!=="pending")return;w();const x=a.shift();if(x===void 0){if(v.length){u=setTimeout(()=>{w(),l==="pending"&&(T(),C())},t.timeout);return}C();return}const h={status:"pending",resource:x,callback:(A,U)=>{k(h,A,U)}};v.push(h),d++,u=setTimeout(D,t.rotate),i(x,e,h.callback)}return setTimeout(D),J}function Ht(t){const e={...Bi,...t};let i=[];function s(){i=i.filter(a=>a().status==="pending")}function n(a,o,l){const d=Ki(e,a,o,(p,u)=>{s(),l&&l(p,u)});return i.push(d),d}function r(a){return i.find(o=>a(o))||null}return{query:n,find:r,setIndex:a=>{e.index=a},getIndex:()=>e.index,cleanup:s}}function ut(){}const ke=Object.create(null);function Vi(t){if(!ke[t]){const e=xe(t);if(!e)return;ke[t]={config:e,redundancy:Ht(e)}}return ke[t]}function Jt(t,e,i){let s,n;if(typeof t=="string"){const r=Pe(t);if(!r)return i(void 0,424),ut;n=r.send;const a=Vi(t);a&&(s=a.redundancy)}else{const r=Je(t);if(r){s=Ht(r);const a=Pe(t.resources?t.resources[0]:"");a&&(n=a.send)}}return!s||!n?(i(void 0,424),ut):s.query(e,n,i)().abort}function ht(){}function Gi(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,Ui(t)}))}function Wi(t){const e=[],i=[];return t.forEach(s=>{(s.match(Rt)?e:i).push(s)}),{valid:e,invalid:i}}function V(t,e,i){function s(){const n=t.pendingIcons;e.forEach(r=>{n&&n.delete(r),t.icons[r]||t.missing.add(r)})}if(i&&typeof i=="object")try{if(!Ut(t,i).length){s();return}}catch(n){console.error(n)}s(),Gi(t)}function gt(t,e){t instanceof Promise?t.then(i=>{e(i)}).catch(()=>{e(null)}):e(t)}function Qi(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:i,prefix:s}=t,n=t.iconsToLoad;if(delete t.iconsToLoad,!n||!n.length)return;const r=t.loadIcon;if(t.loadIcons&&(n.length>1||!r)){gt(t.loadIcons(n,s,i),d=>{V(t,n,d)});return}if(r){n.forEach(d=>{gt(r(d,s,i),p=>{V(t,[d],p?{prefix:s,icons:{[d]:p}}:null)})});return}const{valid:a,invalid:o}=Wi(n);if(o.length&&V(t,o,null),!a.length)return;const l=s.match(Rt)?Pe(i):null;if(!l){V(t,a,null);return}l.prepare(i,s,a).forEach(d=>{Jt(i,d,p=>{V(t,d.icons,p)})})}))}const Be=(t,e)=>{const i=zi(Hi(t,!0,qt()));if(!i.pending.length){let o=!0;return e&&setTimeout(()=>{o&&e(i.loaded,i.missing,i.pending,ht)}),()=>{o=!1}}const s=Object.create(null),n=[];let r,a;return i.pending.forEach(o=>{const{provider:l,prefix:d}=o;if(d===a&&l===r)return;r=l,a=d,n.push(P(l,d));const p=s[l]||(s[l]=Object.create(null));p[d]||(p[d]=[])}),i.pending.forEach(o=>{const{provider:l,prefix:d,name:p}=o,u=P(l,d),v=u.pendingIcons||(u.pendingIcons=new Set);v.has(p)||(v.add(p),s[l][d].push(p))}),n.forEach(o=>{const l=s[o.provider][o.prefix];l.length&&Qi(o,l)}),e?Fi(e,i,n):ht},Yi=t=>new Promise((e,i)=>{const s=typeof t=="string"?se(t,!0):t;if(!s){i(t);return}Be([s||t],n=>{if(n.length&&s){const r=ee(s);if(r){e({...ie,...r});return}}i(t)})});function ft(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Zi(t,e){if(typeof t=="object")return{data:ft(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const r=ft(t);if(r)return{data:r,value:t}}const i=se(t,!0,!0);if(!i)return{value:t};const s=ee(i);if(s!==void 0||!i.prefix)return{value:t,name:i,data:s};const n=Be([i],()=>e(t,i,ee(i)));return{value:t,name:i,loading:n}}let Bt=!1;try{Bt=navigator.vendor.indexOf("Apple")===0}catch{}function Xi(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(Bt||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const es=/(-?[0-9.]*[0-9]+[0-9.]*)/g,ts=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function De(t,e,i){if(e===1)return t;if(i=i||100,typeof t=="number")return Math.ceil(t*e*i)/i;if(typeof t!="string")return t;const s=t.split(es);if(s===null||!s.length)return t;const n=[];let r=s.shift(),a=ts.test(r);for(;;){if(a){const o=parseFloat(r);isNaN(o)?n.push(r):n.push(Math.ceil(o*e*i)/i)}else n.push(r);if(r=s.shift(),r===void 0)return n.join("");a=!a}}function is(t,e="defs"){let i="";const s=t.indexOf("<"+e);for(;s>=0;){const n=t.indexOf(">",s),r=t.indexOf("</"+e);if(n===-1||r===-1)break;const a=t.indexOf(">",r);if(a===-1)break;i+=t.slice(n+1,r).trim(),t=t.slice(0,s).trim()+t.slice(a+1)}return{defs:i,content:t}}function ss(t,e){return t?"<defs>"+t+"</defs>"+e:e}function ns(t,e,i){const s=is(t);return ss(s.defs,e+s.content+i)}const as=t=>t==="unset"||t==="undefined"||t==="none";function Kt(t,e){const i={...ie,...t},s={...Nt,...e},n={left:i.left,top:i.top,width:i.width,height:i.height};let r=i.body;[i,s].forEach(S=>{const _=[],J=S.hFlip,C=S.vFlip;let T=S.rotate;J?C?T+=2:(_.push("translate("+(n.width+n.left).toString()+" "+(0-n.top).toString()+")"),_.push("scale(-1 1)"),n.top=n.left=0):C&&(_.push("translate("+(0-n.left).toString()+" "+(n.height+n.top).toString()+")"),_.push("scale(1 -1)"),n.top=n.left=0);let k;switch(T<0&&(T-=Math.floor(T/4)*4),T=T%4,T){case 1:k=n.height/2+n.top,_.unshift("rotate(90 "+k.toString()+" "+k.toString()+")");break;case 2:_.unshift("rotate(180 "+(n.width/2+n.left).toString()+" "+(n.height/2+n.top).toString()+")");break;case 3:k=n.width/2+n.left,_.unshift("rotate(-90 "+k.toString()+" "+k.toString()+")");break}T%2===1&&(n.left!==n.top&&(k=n.left,n.left=n.top,n.top=k),n.width!==n.height&&(k=n.width,n.width=n.height,n.height=k)),_.length&&(r=ns(r,'<g transform="'+_.join(" ")+'">',"</g>"))});const a=s.width,o=s.height,l=n.width,d=n.height;let p,u;a===null?(u=o===null?"1em":o==="auto"?d:o,p=De(u,l/d)):(p=a==="auto"?l:a,u=o===null?De(p,d/l):o==="auto"?d:o);const v={},$=(S,_)=>{as(_)||(v[S]=_.toString())};$("width",p),$("height",u);const w=[n.left,n.top,l,d];return v.viewBox=w.join(" "),{attributes:v,viewBox:w,body:r}}function Ke(t,e){let i=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const s in e)i+=" "+s+'="'+e[s]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+t+"</svg>"}function rs(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function os(t){return"data:image/svg+xml,"+rs(t)}function Vt(t){return'url("'+os(t)+'")'}const ls=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let he=ls();function cs(t){he=t}function ds(){return he}function ps(t,e){const i=xe(t);if(!i)return 0;let s;if(!i.maxURL)s=0;else{let n=0;i.resources.forEach(a=>{n=Math.max(n,a.length)});const r=e+".json?icons=";s=i.maxURL-n-i.path.length-r.length}return s}function us(t){return t===404}const hs=(t,e,i)=>{const s=[],n=ps(t,e),r="icons";let a={type:r,provider:t,prefix:e,icons:[]},o=0;return i.forEach((l,d)=>{o+=l.length+1,o>=n&&d>0&&(s.push(a),a={type:r,provider:t,prefix:e,icons:[]},o=l.length),a.icons.push(l)}),s.push(a),s};function gs(t){if(typeof t=="string"){const e=xe(t);if(e)return e.path}return"/"}const fs=(t,e,i)=>{if(!he){i("abort",424);return}let s=gs(e.provider);switch(e.type){case"icons":{const r=e.prefix,a=e.icons.join(","),o=new URLSearchParams({icons:a});s+=r+".json?"+o.toString();break}case"custom":{const r=e.uri;s+=r.slice(0,1)==="/"?r.slice(1):r;break}default:i("abort",400);return}let n=503;he(t+s).then(r=>{const a=r.status;if(a!==200){setTimeout(()=>{i(us(a)?"abort":"next",a)});return}return n=501,r.json()}).then(r=>{if(typeof r!="object"||r===null){setTimeout(()=>{r===404?i("abort",r):i("next",n)});return}setTimeout(()=>{i("success",r)})}).catch(()=>{i("next",n)})},ms={prepare:hs,send:fs};function bs(t,e,i){P(i||"",e).loadIcons=t}function vs(t,e,i){P(i||"",e).loadIcon=t}const Se="data-style";let Gt="";function ys(t){Gt=t}function mt(t,e){let i=Array.from(t.childNodes).find(s=>s.hasAttribute&&s.hasAttribute(Se));i||(i=document.createElement("style"),i.setAttribute(Se,Se),t.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Gt}function Wt(){dt("",ms),qt(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const i=t.IconifyPreload,s="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(n=>{try{(typeof n!="object"||n===null||n instanceof Array||typeof n.icons!="object"||typeof n.prefix!="string"||!ct(n))&&console.error(s)}catch{console.error(s)}})}if(t.IconifyProviders!==void 0){const i=t.IconifyProviders;if(typeof i=="object"&&i!==null)for(const s in i){const n="IconifyProviders["+s+"] is invalid.";try{const r=i[s];if(typeof r!="object"||!r||r.resources===void 0)continue;pt(s,r)||console.error(n)}catch{console.error(n)}}}}return{iconLoaded:Mi,getIcon:Li,listIcons:Ri,addIcon:Ft,addCollection:ct,calculateSize:De,buildIcon:Kt,iconToHTML:Ke,svgToURL:Vt,loadIcons:Be,loadIcon:Yi,addAPIProvider:pt,setCustomIconLoader:vs,setCustomIconsLoader:bs,appendCustomStyle:ys,_api:{getAPIConfig:xe,setAPIModule:dt,sendAPIQuery:Jt,setFetch:cs,getFetch:ds,listAPIProviders:Ji}}}const Ie={"background-color":"currentColor"},Qt={"background-color":"transparent"},bt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},vt={"-webkit-mask":Ie,mask:Ie,background:Qt};for(const t in vt){const e=vt[t];for(const i in bt)e[t+"-"+i]=bt[i]}function yt(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function xs(t,e,i){const s=document.createElement("span");let n=t.body;n.indexOf("<a")!==-1&&(n+="<!-- "+Date.now()+" -->");const r=t.attributes,a=Ke(n,{...r,width:e.width+"",height:e.height+""}),o=Vt(a),l=s.style,d={"--svg":o,width:yt(r.width),height:yt(r.height),...i?Ie:Qt};for(const p in d)l.setProperty(p,d[p]);return s}let W;function $s(){try{W=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{W=null}}function ws(t){return W===void 0&&$s(),W?W.createHTML(t):t}function _s(t){const e=document.createElement("span"),i=t.attributes;let s="";i.width||(s="width: inherit;"),i.height||(s+="height: inherit;"),s&&(i.style=s);const n=Ke(t.body,i);return e.innerHTML=ws(n),e.firstChild}function Oe(t){return Array.from(t.childNodes).find(e=>{const i=e.tagName&&e.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function xt(t,e){const i=e.icon.data,s=e.customisations,n=Kt(i,s);s.preserveAspectRatio&&(n.attributes.preserveAspectRatio=s.preserveAspectRatio);const r=e.renderedMode;let a;r==="svg"?a=_s(n):a=xs(n,{...ie,...i},r==="mask");const o=Oe(t);o?a.tagName==="SPAN"&&o.tagName===a.tagName?o.setAttribute("style",a.getAttribute("style")):t.replaceChild(a,o):t.appendChild(a)}function $t(t,e,i){const s=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:s}}function ks(t="iconify-icon"){let e,i;try{e=window.customElements,i=window.HTMLElement}catch{return}if(!e||!i)return;const s=e.get(t);if(s)return s;const n=["icon","mode","inline","noobserver","width","height","rotate","flip"],r=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const o=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");mt(o,l),this._state=$t({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return n.slice(0)}attributeChangedCallback(o){switch(o){case"inline":{const l=this.hasAttribute("inline"),d=this._state;l!==d.inline&&(d.inline=l,mt(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const o=this.getAttribute("icon");if(o&&o.slice(0,1)==="{")try{return JSON.parse(o)}catch{}return o}set icon(o){typeof o=="object"&&(o=JSON.stringify(o)),this.setAttribute("icon",o)}get inline(){return this.hasAttribute("inline")}set inline(o){o?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(o){o?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const o=this._state;if(o.rendered){const l=this._shadowRoot;if(o.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}xt(l,o)}}get status(){const o=this._state;return o.rendered?"rendered":o.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const o=this._state,l=this.getAttribute("icon");if(l!==o.icon.value){this._iconChanged(l);return}if(!o.rendered||!this._visible)return;const d=this.getAttribute("mode"),p=ot(this);(o.attrMode!==d||Ei(o.customisations,p)||!Oe(this._shadowRoot))&&this._renderIcon(o.icon,p,d)}_iconChanged(o){const l=Zi(o,(d,p,u)=>{const v=this._state;if(v.rendered||this.getAttribute("icon")!==d)return;const $={value:d,name:p,data:u};$.data?this._gotIconData($):v.icon=$});l.data?this._gotIconData(l):this._state=$t(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const o=Oe(this._shadowRoot);o&&this._shadowRoot.removeChild(o);return}this._queueCheck()}_gotIconData(o){this._checkQueued=!1,this._renderIcon(o,ot(this),this.getAttribute("mode"))}_renderIcon(o,l,d){const p=Xi(o.data.body,d),u=this._state.inline;xt(this._shadowRoot,this._state={rendered:!0,icon:o,inline:u,customisations:l,attrMode:d,renderedMode:p})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(o=>{const l=o.some(d=>d.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};n.forEach(o=>{o in r.prototype||Object.defineProperty(r.prototype,o,{get:function(){return this.getAttribute(o)},set:function(l){l!==null?this.setAttribute(o,l):this.removeAttribute(o)}})});const a=Wt();for(const o in a)r[o]=r.prototype[o]=a[o];return e.define(t,r),r}const Ss=ks()||Wt(),{iconLoaded:nn,getIcon:an,listIcons:rn,addIcon:on,addCollection:ln,calculateSize:cn,buildIcon:dn,iconToHTML:pn,svgToURL:un,loadIcons:hn,loadIcon:gn,setCustomIconLoader:fn,setCustomIconsLoader:mn,addAPIProvider:bn,_api:vn}=Ss;class Ve extends Error{constructor(e,i){super(i),this.status=e,this.name="ApiRequestError"}}async function m(t,e){const i=await fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!i.ok){const s=await i.json().catch(()=>({error:i.statusText}));throw new Ve(i.status,s.error||i.statusText)}return i.status===204?void 0:i.json()}function Ne(t,e,i=!1){if(e==="telegram"){const s=String(t.get("bot_token")??"");return{type:"telegram",name:t.get("name"),bot_token:i&&!s?void 0:s,chat_id:t.get("chat_id"),default:t.get("default")==="on"}}if(e==="smtp"){const s=String(t.get("username")??""),n=String(t.get("password")??"");return{type:"smtp",name:t.get("name"),host:t.get("host"),port:Number(t.get("port")),security:t.get("security"),username:s||void 0,password:n||void 0,from:t.get("from"),to:t.get("to"),default:t.get("default")==="on"}}return{type:"webhook",name:t.get("name"),url:t.get("url"),headers:i?void 0:{},default:t.get("default")==="on"}}function je(t,e=[],i=!0,s=String(t.get("kind")??"http"),n=[]){const r=String(t.get("url")),a=s==="http"?r:`${s}://${r.replace(/^[a-z][a-z0-9+.-]*:\/\//i,"")}`;return{name:String(t.get("name")),kind:s,url:a,method:String(t.get("method")??"GET"),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),locations:Number(t.get("locations")??1),headers:{},body:null,assertions:n,skip_tls_verification:!1,tls_ca_secret_id:Te(t,"tls_ca_secret_id"),tls_client_certificate_secret_id:Te(t,"tls_client_certificate_secret_id"),tls_client_private_key_secret_id:Te(t,"tls_client_private_key_secret_id"),notification_channel_ids:e,use_default_channels:i}}function Te(t,e){return String(t.get(e)??"")||null}var Ts=Object.defineProperty,As=Object.getOwnPropertyDescriptor,H=(t,e,i,s)=>{for(var n=s>1?void 0:s?As(e,i):e,r=t.length-1,a;r>=0;r--)(a=t[r])&&(n=(s?a(e,i,n):a(n))||n);return s&&n&&Ts(e,i,n),n};let O=class extends R{constructor(){super(...arguments),this.channelKind="webhook",this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await m("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){if(t.preventDefault(),!window.confirm("Create a new single-Node Cluster?"))return;const e=new FormData(t.currentTarget),i=String(e.get("admin_username")??"").trim(),s=String(e.get("admin_password")??"");await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName(),admin_username:i,admin_password:s},{username:i,password:s})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,i=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(i.get("join_link")??"").trim()})}async choose(t,e,i){this.saving=!0,this.error="";try{await m(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster(i)}catch(s){this.fail(s),this.saving=!1}}async waitForCluster(t){for(let e=0;e<120;e+=1){const{promise:i,resolve:s}=Promise.withResolvers();window.setTimeout(s,250),await i;try{t&&await m("/api/v1/auth/login",{method:"POST",body:JSON.stringify(t)});const n=await m("/api/v1/setup");if(n.cluster_ready){this.changed(n);return}}catch(n){if(!t&&n instanceof Ve&&n.status===401){window.location.assign("/");return}}}throw new Error("Cluster setup did not finish within 30 seconds")}async createChannel(t){t.preventDefault();const e=new FormData(t.currentTarget),i=Ne(e,this.channelKind);await this.createResource("/api/v1/channels",i)}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),i=je(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",i)}async createResource(t,e){this.saving=!0;try{await m(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(i){this.fail(i),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await m("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return c`<section class="flow" aria-label="UpGrid setup">
      ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:g}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return c`
      <span class="eyebrow">First-run setup</span><h1>Choose your Cluster</h1>
      <p class="lead">Review this Node’s name, then create a new Cluster or use an invitation to join one.</p>
      <div class="cluster-panel">
        <div class="cluster-identity">
          <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} required /></label>
        </div>
        <form class="cluster-create" @submit=${this.createCluster}>
          <div class="cluster-copy"><h2>Start a new Cluster</h2><p>Create its first replicated administrator identity.</p></div>
          <div class="cluster-create-fields">
            <label>Administrator username<input name="admin_username" autocomplete="username" value="admin" required /></label>
            <label>Administrator password<input name="admin_password" type="password" minlength="12" autocomplete="new-password" required /></label>
          </div>
          <button type="submit" ?disabled=${this.saving}>${this.saving?"Setting up…":"Create new Cluster"}</button>
        </form>
        <div class="cluster-divider"><span>Or</span></div>
        <form class="cluster-join" @submit=${this.joinCluster}>
          <div class="cluster-copy"><h2>Join an existing Cluster</h2><p>Paste an <code>up://</code> Join Token from a current member.</p></div>
          <div class="cluster-join-fields">
            <label>Join Token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
            <button class="secondary" type="submit" ?disabled=${this.saving}>Join Cluster</button>
          </div>
        </form>
      </div>`}renderChannel(){return c`
      <span class="eyebrow">Optional · Step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions to Telegram or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createChannel}>
        <label>Type<select name="type" @change=${t=>this.channelKind=t.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
        <label>Name<input name="name" placeholder="On-call" required /></label>
        ${this.channelKind==="webhook"?c`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:c`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
        <label><span><input name="default" type="checkbox" checked /> Default channel</span></label>
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and continue</button></div>
      </form></div>`}renderTarget(){return c`
      <span class="eyebrow">Optional · Step 3 of 3</span><h1>Monitor your first Target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createTarget}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
        <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label></div>
        ${this.channels.length?c`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>c`<label><span><input name="channel_id" type="checkbox" value=${t.id} /> ${t.name}</span></label>`)}</fieldset>`:g}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`}};O.styles=fe`
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    .flow { width: min(760px, 100%); margin: 0 auto; }
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .lead { margin: 0 0 16px; color: var(--muted); font-size: 15px; }
    .panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    .cluster-panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .cluster-identity, .cluster-create, .cluster-join { padding: 15px 18px; }
    .cluster-identity { border-bottom: 1px solid var(--line); }
    .cluster-create { display: grid; gap: 14px; }
    .cluster-create-fields { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .cluster-create button { justify-self: end; }
    .cluster-copy h2 { margin: 0; font-size: 17px; }
    .cluster-copy p { margin: 2px 0 0; color: var(--muted); }
    .cluster-divider { display: flex; align-items: center; gap: 12px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .12em; }
    .cluster-divider::before, .cluster-divider::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .cluster-join { display: grid; gap: 10px; }
    .cluster-join-fields { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; }
    .cluster-join-fields label { min-width: 0; }
    .cluster-join-fields button { height: 44px; white-space: nowrap; }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: border-color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { display: inline-flex; min-height: 44px; align-items: center; justify-content: center; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .65; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 620px) { .row, .cluster-create-fields, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-create button, .cluster-join button { justify-self: end; } }
    @media (max-height: 650px) and (min-width: 621px) {
      h1 { margin: 2px 0 4px; font-size: 30px; }
      .lead { margin-bottom: 8px; font-size: 13px; }
      .cluster-identity, .cluster-create, .cluster-join { padding: 8px 14px; }
      .cluster-create { grid-template-columns: minmax(0, 1fr) auto; gap: 8px; }
      .cluster-create .cluster-copy { grid-column: 1 / -1; }
      .cluster-create button { align-self: end; }
      .cluster-copy p { display: none; }
      .cluster-join { grid-template-columns: auto minmax(0, 1fr); align-items: end; }
      input, button { min-height: 38px; }
      .cluster-join-fields button { height: 44px; }
    }
  `;H([ve({attribute:!1})],O.prototype,"setup",2);H([f()],O.prototype,"channelKind",2);H([f()],O.prototype,"channels",2);H([f()],O.prototype,"saving",2);H([f()],O.prototype,"error",2);O=H([He("upgrid-setup")],O);var Cs=Object.defineProperty,Es=Object.getOwnPropertyDescriptor,$e=(t,e,i,s)=>{for(var n=s>1?void 0:s?Es(e,i):e,r=t.length-1,a;r>=0;r--)(a=t[r])&&(n=(s?a(e,i,n):a(n))||n);return s&&n&&Cs(e,i,n),n};const Ps={body_contains:"Body contains",body_regex:"Body regex",json_path:"JSONPath",response_header:"Response header",latency:"Latency threshold",script:"Script"};let L=class extends R{constructor(){super(...arguments),this.assertions=[],this.targetId="new",this.draft=[],this.loadedTarget="",this.internals=this.attachInternals()}get value(){return structuredClone(this.draft)}willUpdate(t){t.has("targetId")&&this.loadedTarget!==this.targetId&&(this.loadedTarget=this.targetId,this.draft=structuredClone(this.assertions))}updated(){this.internals.setFormValue(JSON.stringify(this.draft))}formResetCallback(){this.draft=structuredClone(this.assertions),this.internals.setFormValue(JSON.stringify(this.draft))}add(){this.draft=[...this.draft,wt("body_contains")],this.changed()}removeAssertion(t){this.draft=this.draft.filter((e,i)=>i!==t),this.changed()}move(t,e){const i=t+e;if(i<0||i>=this.draft.length)return;const s=[...this.draft];[s[t],s[i]]=[s[i],s[t]],this.draft=s,this.changed()}setKind(t,e){const i=e.currentTarget.value;this.replace(t,wt(i))}set(t,e,i){const s=i.currentTarget,n={...this.draft[t],[e]:e==="max_ms"?Number(s.value):s.value||null};this.replace(t,n)}replace(t,e){this.draft=this.draft.map((i,s)=>s===t?e:i),this.changed()}changed(){this.internals.setFormValue(JSON.stringify(this.draft)),this.dispatchEvent(new Event("input",{bubbles:!0,composed:!0}))}render(){return c`
      <fieldset>
        <legend>Ordered response assertions</legend>
        ${this.draft.length?this.draft.map((t,e)=>this.renderAssertion(t,e)):c`<p class="empty">No response assertions. HTTP status ranges still apply.</p>`}
        <button class="add" type="button" @click=${this.add}>Add assertion</button>
      </fieldset>
    `}renderAssertion(t,e){return c`
      <div class="assertion">
        <label>Type<select aria-label=${`Assertion ${e+1} type`} .value=${t.kind} @change=${i=>this.setKind(e,i)}>${Object.entries(Ps).map(([i,s])=>c`<option value=${i}>${s}</option>`)}</select></label>
        ${this.renderFields(t,e)}
        <div class="actions">
          <button type="button" aria-label=${`Move assertion ${e+1} up`} ?disabled=${e===0} @click=${()=>this.move(e,-1)}>Up</button>
          <button type="button" aria-label=${`Move assertion ${e+1} down`} ?disabled=${e===this.draft.length-1} @click=${()=>this.move(e,1)}>Down</button>
          <button type="button" aria-label=${`Remove assertion ${e+1}`} @click=${()=>this.removeAssertion(e)}>Remove</button>
        </div>
      </div>
    `}renderFields(t,e){switch(t.kind){case"body_contains":return c`<div class="fields single"><label>Required text<input aria-label=${`Assertion ${e+1} required text`} .value=${t.value} required @input=${i=>this.set(e,"value",i)} /></label></div>`;case"body_regex":return c`<div class="fields single"><label>Regular expression<input aria-label=${`Assertion ${e+1} regular expression`} .value=${t.pattern} required @input=${i=>this.set(e,"pattern",i)} /></label></div>`;case"json_path":return c`<div class="fields"><label>Path<input aria-label=${`Assertion ${e+1} JSONPath`} .value=${t.path} placeholder="$.status" required @input=${i=>this.set(e,"path",i)} /></label><label>Expected value (optional)<input aria-label=${`Assertion ${e+1} expected value`} .value=${t.expected??""} @input=${i=>this.set(e,"expected",i)} /></label></div>`;case"response_header":return c`<div class="fields"><label>Header name<input aria-label=${`Assertion ${e+1} header name`} .value=${t.name} placeholder="content-type" required @input=${i=>this.set(e,"name",i)} /></label><label>Exact value (optional)<input aria-label=${`Assertion ${e+1} header value`} .value=${t.value??""} @input=${i=>this.set(e,"value",i)} /></label></div>`;case"latency":return c`<div class="fields single"><label>Maximum milliseconds<input aria-label=${`Assertion ${e+1} maximum milliseconds`} type="number" min="1" .value=${String(t.max_ms)} required @input=${i=>this.set(e,"max_ms",i)} /></label></div>`;case"script":return c`<div class="fields single"><label>Boolean Rhai expression<textarea aria-label=${`Assertion ${e+1} script`} required @input=${i=>this.set(e,"source",i)}>${t.source}</textarea></label></div>`;default:return g}}};L.formAssociated=!0;L.styles=fe`
    :host { display: grid; gap: 10px; margin: 10px 0; }
    fieldset { display: grid; gap: 8px; border: 1px solid var(--line); border-radius: 8px; padding: 10px; }
    legend { color: var(--muted); font-size: 13px; padding: 0 4px; }
    .assertion { display: grid; grid-template-columns: minmax(140px, 0.7fr) minmax(180px, 1.3fr) auto; gap: 8px; align-items: end; }
    .fields { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .fields.single { grid-template-columns: 1fr; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; }
    input, select, textarea { box-sizing: border-box; width: 100%; border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 9px; font: inherit; }
    textarea { min-height: 72px; resize: vertical; font-family: ui-monospace, monospace; }
    .actions { display: flex; gap: 4px; }
    button { border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 10px; cursor: pointer; }
    button:disabled { cursor: default; opacity: 0.45; }
    .add { justify-self: start; }
    .empty { margin: 0; color: var(--muted); font-size: 13px; }
    @media (max-width: 720px) { .assertion { grid-template-columns: 1fr; } .fields { grid-template-columns: 1fr; } }
  `;$e([ve({attribute:!1})],L.prototype,"assertions",2);$e([ve({attribute:"target-id"})],L.prototype,"targetId",2);$e([f()],L.prototype,"draft",2);L=$e([He("http-assertion-editor")],L);function wt(t){switch(t){case"body_contains":return{kind:t,value:""};case"body_regex":return{kind:t,pattern:""};case"json_path":return{kind:t,path:"$",expected:null};case"response_header":return{kind:t,name:"",value:null};case"latency":return{kind:t,max_ms:1e3};case"script":return{kind:t,source:"status == 200"}}}const Ds={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},Is={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},Os={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var Ns=Object.defineProperty,y=(t,e,i,s)=>{for(var n=void 0,r=t.length-1,a;r>=0;r--)(a=t[r])&&(n=a(e,i,n)||n);return n&&Ns(e,i,n),n};const oe=["system","dark","bright"],_t={system:Is,dark:Ds,bright:Os},Ge={overview:"/",alerts:"/alerts",cluster:"/cluster"};function js(t,e){if(!e)return{tone:"pending",label:"connecting"};const i=t.filter(n=>!n.paused);if(!i.length)return{tone:"pending",label:"ready"};const s=i.filter(n=>n.availability==="down"||n.consecutive_failures>0).length;return s?s===i.length?{tone:"down",label:"down"}:{tone:"degraded",label:"partially down"}:{tone:"up",label:"up"}}function kt(){return Object.entries(Ge).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function Rs(){const t=localStorage.getItem("upgrid-theme");return oe.includes(t)?t:"system"}class b extends R{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.transitions=[],this.secrets=[],this.joinTokens=[],this.identities=[],this.apiTokens=[],this.authReady=!1,this.newApiToken="",this.error="",this.live=!1,this.saving=!1,this.historyLoading=!1,this.channelKind="webhook",this.channelTestMessage="",this.testingChannel=!1,this.joinCommand="",this.alertSearch="",this.alertDeliveryFilter="all",this.alertKindFilter="all",this.alertAcknowledgedFilter="all",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=kt(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!1,this.theme=Rs(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=kt()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await m("/api/v1/setup");e.cluster_ready&&(this.session=await m("/api/v1/auth/session")),await this.activate(e)}catch(e){(!(e instanceof Ve)||e.status!==401)&&(this.error=e instanceof Error?e.message:String(e))}this.authReady=!0}async activate(e){if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}async login(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0,this.error="";try{this.session=await m("/api/v1/auth/login",{method:"POST",body:JSON.stringify({username:String(i.get("username")??""),password:String(i.get("password")??"")})}),await this.activate(await m("/api/v1/setup"))}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async logout(){await m("/api/v1/auth/logout",{method:"POST"}),this.events?.close(),this.session=void 0,this.live=!1,this.setupMode=!1,window.history.replaceState(null,"","/")}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=oe[(oe.indexOf(this.theme)+1)%oe.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.channels,this.alerts,this.transitions,this.secrets,this.cluster,this.joinTokens,this.identities,this.apiTokens]=await Promise.all([m("/api/v1/targets"),m("/api/v1/channels"),m("/api/v1/alerts"),m("/api/v1/transitions"),m("/api/v1/secrets"),m("/api/v1/cluster"),m("/api/v1/join-tokens"),m("/api/v1/identities"),m("/api/v1/api-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.targetHistory=void 0,this.historyLoading=!0,this.loadTargetHistory(e.id),this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),s=i?.querySelector("form");s&&(this.detailInitialState=this.detailFormState(s)),i?.showModal()})}async loadTargetHistory(e){try{const i=await m(`/api/v1/targets/${e}/history?limit=720`);this.selected?.id===e&&(this.targetHistory=i)}catch(i){this.selected?.id===e&&(this.error=i instanceof Error?i.message:String(i))}finally{this.selected?.id===e&&(this.historyLoading=!1)}}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0,this.targetHistory=void 0,this.historyLoading=!1}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,i){e.preventDefault(),this.activeSection=i,window.history.pushState(null,"",Ge[i])}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const i=e.currentTarget,s=i.form?.elements.namedItem("max_redirects");s&&(s.disabled=!i.checked),i.form&&this.compareDetailForm(i.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}}y([f()],b.prototype,"targets");y([f()],b.prototype,"channels");y([f()],b.prototype,"alerts");y([f()],b.prototype,"transitions");y([f()],b.prototype,"secrets");y([f()],b.prototype,"cluster");y([f()],b.prototype,"joinTokens");y([f()],b.prototype,"identities");y([f()],b.prototype,"apiTokens");y([f()],b.prototype,"session");y([f()],b.prototype,"authReady");y([f()],b.prototype,"newApiToken");y([f()],b.prototype,"error");y([f()],b.prototype,"live");y([f()],b.prototype,"saving");y([f()],b.prototype,"selected");y([f()],b.prototype,"targetHistory");y([f()],b.prototype,"historyLoading");y([f()],b.prototype,"channelKind");y([f()],b.prototype,"editingChannel");y([f()],b.prototype,"channelTestMessage");y([f()],b.prototype,"testingChannel");y([f()],b.prototype,"joinCommand");y([f()],b.prototype,"alertSearch");y([f()],b.prototype,"alertDeliveryFilter");y([f()],b.prototype,"alertKindFilter");y([f()],b.prototype,"alertAcknowledgedFilter");y([f()],b.prototype,"search");y([f()],b.prototype,"statusFilter");y([f()],b.prototype,"sort");y([f()],b.prototype,"selectedIds");y([f()],b.prototype,"activeSection");y([f()],b.prototype,"copied");y([f()],b.prototype,"setupMode");y([f()],b.prototype,"setup");y([f()],b.prototype,"warningDismissed");y([f()],b.prototype,"unlimitedUses");y([f()],b.prototype,"theme");y([f()],b.prototype,"detailDirty");class Ms extends b{async createTarget(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i),n=i.querySelector("http-assertion-editor")?.value??[],r=je(s,s.getAll("channel_id").map(String),s.get("use_default_channels")==="on",void 0,n);this.saving=!0;try{await m("/api/v1/targets",{method:"POST",body:JSON.stringify(r)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=new FormData(e.currentTarget),s=e.currentTarget.querySelector("http-assertion-editor")?.value??[];let n=`/api/v1/nodes/${this.selected.id}`,r={name:String(i.get("name"))};if(this.selected.kind==="http"){const a=i.get("follow_redirects")==="on";n=`/api/v1/targets/${this.selected.id}`,r={name:String(i.get("name")),kind:"http",url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:String(i.get("statuses")).split(",").map(o=>{const[l,d]=o.trim().split("-").map(Number);return{start:l,end:d||l}}),follow_redirects:a,max_redirects:a?Number(i.get("max_redirects")):0,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),locations:Number(i.get("locations")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([o,l])=>[o,l.kind==="literal"?l.value:{secret_id:l.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,assertions:s,skip_tls_verification:i.get("skip_tls_verification")==="on",tls_ca_secret_id:String(i.get("tls_ca_secret_id")??"")||null,tls_client_certificate_secret_id:String(i.get("tls_client_certificate_secret_id")??"")||null,tls_client_private_key_secret_id:String(i.get("tls_client_private_key_secret_id")??"")||null,notification_channel_ids:i.getAll("channel_id").map(String),use_default_channels:i.get("use_default_channels")==="on"}}this.selected.kind!=="http"&&this.selected.kind!=="node"&&(n=`/api/v1/targets/${this.selected.id}`,r=je(i,i.getAll("channel_id").map(String),i.get("use_default_channels")==="on",this.selected.kind,s)),this.saving=!0;try{await m(n,{method:"PUT",body:JSON.stringify(r)}),this.closeDetailDialog(),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await m(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await m(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i);this.saving=!0;try{await m("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:s.get("name"),value:s.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i),n=this.editingChannel,r=Ne(s,this.channelKind,n!==void 0);this.saving=!0;try{await m(n?`/api/v1/channels/${n.id}`:"/api/v1/channels",{method:n?"PUT":"POST",body:JSON.stringify(r)}),await this.refresh(),i.reset(),this.editingChannel=void 0,this.channelKind="webhook",this.channelTestMessage="",this.closeDialog("channel-dialog")}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}openChannelDialog(e){this.editingChannel=e,this.channelKind=e?.kind??"webhook",this.channelTestMessage="",this.showDialog("channel-dialog")}async setChannelDefault(e,i){try{await m(`/api/v1/channels/${e.id}/default`,{method:"PUT",body:JSON.stringify({default:i})}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}}async testChannel(e){const i=e.currentTarget.form;if(!(!i||![...i.querySelectorAll("[data-test-required]")].every(n=>n.reportValidity()))){this.testingChannel=!0,this.channelTestMessage="";try{const n=Ne(new FormData(i),this.channelKind);await m("/api/v1/channels/test",{method:"POST",body:JSON.stringify(n)}),this.channelTestMessage="Test sent"}catch(n){const r=n instanceof Error?n.message:String(n);this.channelTestMessage=`Test failed: ${r}`}finally{this.testingChannel=!1}}}openTokenDialog(){this.unlimitedUses=!1,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0;try{const s=await m("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(i.get("expiration_days"))*86400,max_uses:this.unlimitedUses?null:Number(i.get("max_uses"))})});this.joinCommand=`upgrid --join '${s.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async createIdentity(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i);await this.saveResource(async()=>{await m("/api/v1/identities",{method:"POST",body:JSON.stringify({username:String(s.get("username")??""),password:String(s.get("password")??"")})}),i.reset()})}async updateIdentity(e,i){i.preventDefault();const s=new FormData(i.currentTarget),n=String(s.get("password")??"");await this.saveResource(async()=>{await m(`/api/v1/identities/${e.id}`,{method:"PUT",body:JSON.stringify({username:String(s.get("username")??""),password:n||null})}),e.id===this.session?.identity_id&&n&&await this.logout()})}async deleteIdentity(e){window.confirm(`Delete identity ${e.username}? Its API Tokens will also be revoked.`)&&await this.saveResource(()=>m(`/api/v1/identities/${e.id}`,{method:"DELETE"}))}async createApiToken(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i);await this.saveResource(async()=>{const n=Number(s.get("expires_in_days")),r=await m("/api/v1/api-tokens",{method:"POST",body:JSON.stringify({name:String(s.get("name")??""),expires_in_seconds:n?n*86400:null})});this.newApiToken=r.value,i.reset()})}async revokeApiToken(e){window.confirm(`Revoke API Token ${e.name}?`)&&await this.saveResource(()=>m(`/api/v1/api-tokens/${e.id}`,{method:"DELETE"}))}async setNodeDrain(e,i){await this.saveResource(()=>m(`/api/v1/nodes/${e.id}/drain`,{method:"PUT",body:JSON.stringify({draining:i,force:!1})}))}async removeNode(e,i){const s=i?`Replace failed Node ${e.name}? Confirm that it is permanently stopped. Its assignments will be released immediately.`:`Remove drained Node ${e.name} from the Cluster?`;window.confirm(s)&&(await this.saveResource(()=>m(`/api/v1/nodes/${e.id}?force=${i}`,{method:"DELETE"})),i&&!this.error&&this.openTokenDialog())}async acknowledgeAlert(e){await this.updateAlert("acknowledge",e)}async retryAlert(e){await this.updateAlert("retry",e)}async updateAlert(e,i){await this.saveResource(()=>m(`/api/v1/alerts/${e}`,{method:"POST",body:JSON.stringify({target_id:i.target_id,channel_id:i.channel_id,scheduled_at_ms:i.scheduled_at_ms,kind:i.kind})}))}async saveResource(e){this.saving=!0,this.error="";try{await e(),this.session&&await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async setupChanged(e){const i=e.detail;if(this.setup=i,this.setupMode=i.setup,window.history.replaceState(null,"",i.path),i.setup){i.cluster_ready&&(this.session=await m("/api/v1/auth/session"),await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await m(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const i=Object.assign(document.createElement("textarea"),{value:this.joinCommand});i.style.cssText="position: fixed; opacity: 0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const s=new Set(this.selectedIds);i?s.add(e):s.delete(e),this.selectedIds=s}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>m(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>m(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,i,s){if(window.confirm(`Delete ${s}?`))try{await m(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}}}const Ls={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5L2 22l1.5-5.5Zm-2 2l4 4"/>'};function Us(t,e){const i=e.search.trim().toLocaleLowerCase();return(!i||`${t.target_name} ${t.channel_name}`.toLocaleLowerCase().includes(i))&&(e.delivery==="all"||t.delivery===e.delivery)&&(e.kind==="all"||t.kind===e.kind)&&(e.acknowledged==="all"||(e.acknowledged==="yes"?t.acknowledged_at_ms!==null:t.acknowledged_at_ms===null))}function qs(t){return t.delivery==="pending"?t.next_attempt_at_ms===null?`${t.attempts} attempts`:`${t.attempts} attempts · next ${new Date(t.next_attempt_at_ms).toLocaleString()}`:t.delivery==="failed"?t.diagnostic??"Delivery failed":t.completed_at_ms===null?"Delivered":`Delivered ${new Date(t.completed_at_ms).toLocaleString()}`}function Fs(t,e,i,s,n,r){const a=t.filter(o=>Us(o,s));return c`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${r.create}>Add channel</button>
    </section>
    <section class="panel alert-history" aria-label="Alert history">
      <div class="panel-head"><h2>Notification deliveries</h2><span class="meta">${a.length} of ${t.length} alerts</span></div>
      <div class="alert-filters">
        <label>Search<input type="search" .value=${s.search} placeholder="Target or channel" @input=${o=>r.setSearch(o.target.value)} /></label>
        <label>Delivery<select .value=${s.delivery} @change=${o=>r.setDelivery(o.target.value)}><option value="all">All</option><option value="pending">Pending</option><option value="delivered">Delivered</option><option value="failed">Failed</option></select></label>
        <label>Transition<select .value=${s.kind} @change=${o=>r.setKind(o.target.value)}><option value="all">All</option><option value="down">Down</option><option value="recovered">Recovered</option></select></label>
        <label>Acknowledged<select .value=${s.acknowledged} @change=${o=>r.setAcknowledged(o.target.value)}><option value="all">All</option><option value="no">No</option><option value="yes">Yes</option></select></label>
      </div>
      ${a.length?a.map(o=>c`
                <div class="resource alert-resource">
                  <div class="alert-summary">
                    <div class="channel-title">
                      <strong>${o.target_name}</strong>
                      <span class=${`badge ${o.kind==="recovered"?"up":"down"}`}>${o.kind}</span>
                      <span class="badge">${o.delivery}</span>
                      ${o.acknowledged_at_ms===null?g:c`<span class="badge">acknowledged</span>`}
                    </div>
                    <code>${o.channel_name} · ${new Date(o.scheduled_at_ms).toLocaleString()}</code>
                    <span class="meta">${qs(o)}</span>
                  </div>
                  <div class="alert-actions">
                    ${o.delivery==="failed"?c`<button class="button secondary" ?disabled=${n} @click=${()=>r.retry(o)}>Retry</button>`:g}
                    ${o.acknowledged_at_ms===null?c`<button class="button secondary" ?disabled=${n} @click=${()=>r.acknowledge(o)}>Acknowledge</button>`:g}
                  </div>
                </div>
              `):c`<div class="empty">No alerts match these filters.</div>`}
    </section>
    <div class="page-columns">
      <section class="panel" aria-label="Availability history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${e.length} events</span></div>
        ${e.length?e.map(o=>{const l=o.kind==="recovered"?"up":"down";return c`
                <div class="resource">
                  <div class="transition-main">
                    <span class=${`state ${l}`} aria-hidden="true"></span>
                    <div>
                      <strong>${o.target_name}</strong>
                      <code>${new Date(o.scheduled_at_ms).toLocaleString()}</code>
                    </div>
                  </div>
                  <span class=${`badge ${l}`}>${o.kind}</span>
                </div>
              `}):c`<div class="empty">No availability transitions.</div>`}
      </section>
      <section class="panel" aria-label="Notification channels">
        <div class="panel-head"><h2>Notification channels</h2><span class="meta">${i.length} configured</span></div>
        ${i.length?i.map(o=>c`
              <div class="resource channel-resource">
                <div class="channel-summary"><div class="channel-title"><strong>${o.name}</strong><span class="badge">${o.kind}</span></div><code>${o.destination}</code></div>
                <div class="channel-actions">
                  <label class="switch"><span>Default</span><input type="checkbox" role="switch" aria-label=${`Default channel ${o.name}`} .checked=${o.default} @change=${l=>r.setDefault(o,l.target.checked)} /></label>
                  <button class="button secondary icon-button" aria-label=${`Edit channel ${o.name}`} title=${`Edit ${o.name}`} @click=${()=>r.edit(o)}><iconify-icon .icon=${Ls} aria-hidden="true"></iconify-icon></button>
                  <button class="button danger icon-button" aria-label=${`Delete channel ${o.name}`} title=${`Delete ${o.name}`} @click=${()=>r.remove(o)}><iconify-icon .icon=${de} aria-hidden="true"></iconify-icon></button>
                </div>
              </div>
            `):c`<div class="empty">No notification channels.</div>`}
      </section>
    </div>
  `}function zs(t,e,i){return c`
    <main class="shell setup-shell">
      <header>
        <div class="brand"><img src="/favicon.svg" alt="" /><div><strong>UpGrid</strong><span>Distributed service monitoring</span></div></div>
      </header>
      <section class="panel auth-panel" aria-labelledby="login-title">
        <form class="choice" @submit=${i.login}>
          <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated Operator Identity.</p></div>
          ${e?c`<div class="notice" role="alert">${e}</div>`:g}
          <label>Username<input name="username" autocomplete="username" required autofocus /></label>
          <label>Password<input name="password" type="password" autocomplete="current-password" required /></label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${t}>${t?"Signing in…":"Sign in"}</button></div>
        </form>
      </section>
    </main>`}function Hs(t,e,i,s,n,r){return c`
    <div class="page-columns access-panels">
      <section class="panel" aria-label="Operator Identities">
        <div class="panel-head"><h2>Operator Identities</h2><span class="meta">${t.length} administrators</span></div>
        ${t.map(a=>c`
            <div class="resource access-resource">
              <form class="access-form" @submit=${o=>r.updateIdentity(a,o)}>
                <label>Username<input name="username" .value=${a.username} required /></label>
                <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
                <button class="button secondary" type="submit" ?disabled=${n}>Save</button>
              </form>
              <button class="button danger" type="button" ?disabled=${a.id===i||n} @click=${()=>r.deleteIdentity(a)}>Delete</button>
            </div>`)}
        <form class="choice compact-form" @submit=${r.createIdentity}>
          <h3>Add administrator</h3>
          <label>Username<input name="username" required /></label>
          <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
          <button class="button" type="submit" ?disabled=${n}>Add identity</button>
        </form>
      </section>
      <section class="panel" aria-label="API Tokens">
        <div class="panel-head"><h2>API Tokens</h2><span class="meta">${e.length} active</span></div>
        ${s?c`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${s}</code><button class="button secondary" @click=${r.dismissToken}>Dismiss</button></div>`:g}
        ${e.length?e.map(a=>c`<div class="resource"><div><strong>${a.name}</strong><code>${a.expires_at_ms?`Expires ${new Date(a.expires_at_ms).toLocaleString()}`:"Never expires"}</code></div><button class="button danger" @click=${()=>r.revokeApiToken(a)}>Revoke</button></div>`):c`<div class="empty">No API Tokens.</div>`}
        <form class="choice compact-form" @submit=${r.createApiToken}>
          <h3>Create API Token</h3>
          <label>Name<input name="name" placeholder="Automation" required /></label>
          <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
          <button class="button" type="submit" ?disabled=${n}>Create API Token</button>
        </form>
      </section>
    </div>`}const Js={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4m0-4h.01"/></g>'},Bs=fe`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { display: flex; align-items: center; gap: 3px; }
  .help-tooltip-wrap { position: relative; display: inline-flex; align-items: center; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: help; transition: background-color 160ms ease, color 160ms ease; }
  .help-tooltip-trigger:hover { background: var(--panel-2); color: var(--text); }
  .help-tooltip-trigger iconify-icon { width: 16px; height: 16px; font-size: 16px; }
  .help-tooltip { position: absolute; top: calc(100% + 6px); left: -60px; z-index: 10; width: 280px; max-width: calc(100vw - 64px); border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--text); box-shadow: 0 10px 30px var(--dialog-shadow); padding: 9px 10px; font-size: 12px; font-weight: 400; line-height: 1.45; opacity: 0; visibility: hidden; transform: translateY(-3px); pointer-events: none; transition: opacity 140ms ease, transform 140ms ease, visibility 140ms; }
  .help-tooltip-wrap:hover .help-tooltip, .help-tooltip-wrap:focus-within .help-tooltip { opacity: 1; visibility: visible; transform: translateY(0); }
`;function ge(t,e,i){return c`
    <span class="help-tooltip-wrap">
      <button class="help-tooltip-trigger" type="button" aria-label=${e} aria-describedby=${t}>
        <iconify-icon .icon=${Js} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip" id=${t} role="tooltip">${i}</span>
    </span>
  `}function Ks(t,e){return t==="webhook"?c`<label
      >Webhook URL<input
        name="url"
        type="url"
        placeholder="https://hooks.example.com/upgrid"
        .value=${e?.destination??""}
        data-test-required
        required
    /></label>`:t==="telegram"?c`
      <label
        ><span class="title-with-help"
          >Bot token
          ${ge("telegram-token-help","About Telegram bot token storage",e?"Leave this blank to keep the automatically managed Secret, or enter a replacement token.":"Creating the Channel encrypts this token as an automatically managed Secret. Test sends use the entered value without storing it.")}</span
        ><input
          name="bot_token"
          type="password"
          autocomplete="off"
          placeholder=${e?"Leave blank to keep current token":""}
          ?required=${e===void 0}
      /></label>
      <label
        >Chat ID<input name="chat_id" .value=${e?.destination??""} data-test-required required
      /></label>
    `:c`
    <label
      >SMTP host<input name="host" placeholder="smtp.example.com" .value=${e?.destination??""} required
    /></label>
    <div class="row">
      <label
        >Port<input
          name="port"
          type="number"
          min="1"
          max="65535"
          .value=${String(e?.port??587)}
          required
      /></label>
      <label
        >Security<select name="security" .value=${e?.security??"start_tls"}>
          <option value="start_tls">STARTTLS</option>
          <option value="tls">Implicit TLS</option>
          <option value="none">Plaintext</option>
        </select></label
      >
    </div>
    <label
      >Username<input name="username" autocomplete="username" .value=${e?.username??""}
    /></label>
    <div class="form-field">
      <div class="title-with-help">
        <label for="smtp-password">Password</label>
        ${ge("smtp-password-help","About SMTP password storage",e?"Leave this blank to keep the automatically managed Secret. Clear the username to disable authentication.":"Enter a username and password together to enable authentication. The password is encrypted as an automatically managed Secret.")}
      </div>
      <input
        id="smtp-password"
        name="password"
        type="password"
        autocomplete="off"
        placeholder=${e?"Leave blank to keep current password":"Optional"}
      />
    </div>
    <label
      >From<input name="from" placeholder="UpGrid <upgrid@example.com>" .value=${e?.from??""} required
    /></label>
    <label
      >Recipient<input name="to" placeholder="on-call@example.com" .value=${e?.to??""} required
    /></label>
  `}const Vs={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5c.08-1.25-.27-2.48-1-3.5c.28-1.15.28-2.35 0-3.5c0 0-1 0-3 1.5c-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5c-.39.49-.68 1.05-.85 1.65c-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></g>'},Gs={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M21.54 15H17a2 2 0 0 0-2 2v4.54M7 3.34V5a3 3 0 0 0 3 3v0a2 2 0 0 1 2 2v0c0 1.1.9 2 2 2v0a2 2 0 0 0 2-2v0c0-1.1.9-2 2-2h3.17M11 21.95V18a2 2 0 0 0-2-2v0a2 2 0 0 1-2-2v-1a2 2 0 0 0-2-2H2.05"/><circle cx="12" cy="12" r="10"/></g>'};function Ae(){return c`
    <footer aria-label="Project information">
      <div class="footer-links">
        <a href="https://miao.dev">A Project by Pop</a>
        <span aria-hidden="true">|</span>
        <a href="https://github.com/George-Miao/UpGrid">
          <iconify-icon .icon=${Vs} aria-hidden="true"></iconify-icon>GitHub
        </a>
        <span aria-hidden="true">|</span>
        <a href="https://upgrid.rs">
          <iconify-icon .icon=${Gs} aria-hidden="true"></iconify-icon>upgrid.rs
        </a>
      </div>
      <div class="footer-powered">
        Proudly powered by <a href="https://compio.rs/">Compio</a> and
        <a href="https://github.com/databendlabs/openraft">OpenRaft</a>
      </div>
    </footer>
  `}function Yt(t,e=[],i=!0){return c`
    <fieldset class="channel-fields">
      <legend>Notification channels</legend>
      <label class="switch">
        <span>Use default channels</span>
        <input
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${i}
          @change=${n=>{const r=n.currentTarget;r.closest("fieldset")?.querySelectorAll('input[data-default="true"]').forEach(o=>{o.disabled=r.checked,o.checked=r.checked||o.dataset.explicit==="true"}),r.form?.dispatchEvent(new Event("input",{bubbles:!0}))}}
        />
      </label>
      <div class="channel-options">
        ${t.map(n=>{const r=e.includes(n.id),a=i&&n.default;return c`
            <label class="check">
              <input
                name="channel_id"
                type="checkbox"
                value=${n.id}
                data-default=${String(n.default)}
                data-explicit=${String(r)}
                .checked=${r||a}
                ?disabled=${a}
                @change=${o=>{const l=o.currentTarget;l.dataset.explicit=String(l.checked)}}
              />
              ${n.name} <span class="badge">${n.kind}</span>
            </label>
          `})}
      </div>
    </fieldset>`}function Zt(t,e=null,i=null,s=null){const n=r=>c`
    <option value="">Not configured</option>
    ${t.map(a=>c`<option value=${a.id} ?selected=${a.id===r}>${a.name}</option>`)}
  `;return c`
    <fieldset class="tls-fields">
      <legend>HTTPS trust and mutual TLS</legend>
      <label>Custom CA bundle Secret<select name="tls_ca_secret_id">${n(e)}</select></label>
      <div class="row">
        <label>Client certificate Secret<select name="tls_client_certificate_secret_id">${n(i)}</select></label>
        <label>Client private key Secret<select name="tls_client_private_key_secret_id">${n(s)}</select></label>
      </div>
      <p class="meta">PEM values stay encrypted. Client certificate and private key must be configured together.</p>
    </fieldset>
  `}const Xt={http:"https://example.com/health",tcp:"database.internal:5432",dns:"service.internal",icmp:"192.0.2.10",tls:"example.com:443"};function ei(t,e){const i=t.elements.namedItem("url");i&&(i.placeholder=Xt[e],i.type=e==="http"?"url":"text");const s=t.querySelector("[data-http-options]");s&&(s.hidden=e!=="http");const n=t.elements.namedItem("method");n&&(n.disabled=e!=="http",n.disabled&&(n.value="GET"))}function Ws(t){const e=t.currentTarget;e.form&&ei(e.form,e.value)}function Qs(t){const e=t.currentTarget;queueMicrotask(()=>ei(e,"http"))}function Ys(t,e,i,s){return c`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${s.backdrop}>
      <div class="dialog-head"><div class="title-with-help"><h2 id="add-target-title">Add target</h2>${ge("target-secret-help","About Target Secrets","Advanced HTTP Targets can reference reusable Secrets for headers, request bodies, custom CA bundles, and mutual-TLS identities.")}</div><p>Start monitoring a service.</p></div>
      <form @submit=${s.create} @reset=${Qs}>
        <label>Name<input name="name" placeholder="Production API" required autofocus /></label>
        <div class="row">
          <label>Type<select name="kind" @change=${Ws}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select></label>
          <label>URL / endpoint<input name="url" type="url" placeholder=${Xt.http} required /></label>
        </div>
        <div data-http-options><label>Method<input name="method" value="GET" required /></label><http-assertion-editor name="assertions" target-id="new"></http-assertion-editor>${Zt(e)}</div>
        <div class="row">
          <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
        </div>
        <div class="row">
          <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
          <label>Evaluation locations<input name="locations" type="number" min="1" max="32" value="1" required /></label>
        </div>
        ${Yt(t)}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${s.close}>Cancel</button>
          <button class="button" type="submit" ?disabled=${i}>${i?"Creating…":"Create target"}</button>
        </div>
      </form>
    </dialog>`}function Zs(t,e,i,s,n,r,a,o,l){const d=t.kind==="node",p=t.kind==="http",u=t.accepted_statuses.map(h=>h.start===h.end?h.start:`${h.start}-${h.end}`).join(","),v=t.history.slice(0,30).reverse(),$=Math.max(1,...v.map(h=>h.latency_ms)),w=e?.items??[],S=w.reduce((h,A)=>h+A.samples,0),_=w.reduce((h,A)=>h+A.successes,0),J=w.reduce((h,A)=>h+A.latency_total_ms,0),C=S?`${(_/S*100).toFixed(2)}%`:"—",T=new Map(r.map(h=>[h.id,h.name])),k=h=>new Date(h).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),D=h=>h>=1e3?`${(h/1e3).toFixed(h>=1e4?0:1)} s`:`${Math.round(h)} ms`,x=S?D(J/S):"—";return c`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${l.backdrop}>
      <div class="dialog-head">
        <h2 id="target-detail-title">${d?"Node details":"Target details"}</h2>
        <button class="button secondary icon-button dialog-close" type="button" aria-label=${`Close ${d?"Node":"Target"} details`} title="Close" @click=${l.close}><iconify-icon .icon=${It} aria-hidden="true"></iconify-icon></button>
      </div>
      <form @submit=${l.update} @input=${l.changed}>
        <label>Name<input name="name" .value=${t.name} required /></label>
        ${d?c`<label>RPC URL<input .value=${t.url} disabled /></label>`:c`
              <div class="row"><label>Type<input .value=${t.kind.toUpperCase()} disabled /></label><label>URL / endpoint<input name="url" .value=${t.url} required /></label></div>
              ${p?c`
                    <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${u} required /></label></div>
                    <http-assertion-editor name="assertions" target-id=${t.id} .assertions=${t.assertions}></http-assertion-editor>
                    <div class="row"><label class="check"><input name="follow_redirects" type="checkbox" .checked=${t.follow_redirects} @change=${l.redirects} />Follow redirects</label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
                    <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${t.skip_tls_verification} />Skip TLS verification</label>
                    ${Zt(o,t.tls_ca_secret_id,t.tls_client_certificate_secret_id,t.tls_client_private_key_secret_id)}
                  `:g}
              <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
              <div class="row"><label>Failures before Down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Evaluation locations<input name="locations" type="number" min="1" max="32" .value=${String(t.locations)} required /></label></div>
              ${Yt(a,t.notification_channel_ids,t.use_default_channels)}
            `}
        <div class="dialog-actions">
          ${d?g:c`<div class="danger-actions">
            <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${l.delete}><iconify-icon .icon=${de} aria-hidden="true"></iconify-icon></button>
            <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>l.pause(!t.paused)}><iconify-icon .icon=${t.paused?Dt:Pt} aria-hidden="true"></iconify-icon></button>
          </div>`}
          <button class="button" type="submit" aria-busy=${s?"true":"false"} ?disabled=${s||!n}>Save changes</button>
        </div>
      </form>
      <section class="history">
        <div class="history-head"><h3>Long-term summary</h3><span class="meta">Last 30 days</span></div>
        ${i?c`<p class="meta">Loading long-term history…</p>`:S?c`
                <div class="history-summary" aria-label="Long-term evaluation summary">
                  <div><span>Availability</span><strong>${C}</strong></div>
                  <div><span>Average latency</span><strong>${x}</strong></div>
                  <div><span>Evaluations</span><strong>${S.toLocaleString()}</strong></div>
                </div>
              `:c`<p class="meta">No long-term history recorded yet.</p>`}
      </section>
      <section class="history">
        <div class="history-head"><h3>Evaluation history</h3>${v.length?c`<span class="meta">Latest ${v.length}</span>`:g}</div>
        ${v.length?c`
          <div class="chart-plot">
            <div class="chart-scale" aria-hidden="true"><span>${D($)}</span><span>${D($/2)}</span><span>0 ms</span></div>
            <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${D($)}`}>
              ${v.map(h=>{const A=h.succeeded?"Passed":"Failed",U=d||!p?h.succeeded?"reachable":"unreachable":h.status_code===null?"network error":`HTTP ${h.status_code}`,E=T.get(h.executor_node_id)??`Node ${h.executor_node_id.slice(0,8)}`,We=`${A} at ${new Date(h.recorded_at_ms).toLocaleString()}: ${h.latency_ms} ms, ${U}. Executed by ${E}`;return c`<span class="history-bar ${h.succeeded?"up":"down"}" role="listitem" aria-label=${We} title=${We} style=${`height: ${Math.max(8,h.latency_ms/$*100)}%`}></span>`})}
            </div>
          </div>
          <div class="chart-axis"><span>${k(v[0].recorded_at_ms)}</span><span>${k(v[v.length-1].recorded_at_ms)}</span></div>
          <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
        `:c`<p class="meta">No evaluations recorded yet.</p>`}
      </section>
    </dialog>`}var Xs=Object.getOwnPropertyDescriptor,en=(t,e,i,s)=>{for(var n=s>1?void 0:s?Xs(e,i):e,r=t.length-1,a;r>=0;r--)(a=t[r])&&(n=a(n)||n);return n};let Re=class extends Ms{render(){const t=this.targets.filter(a=>a.availability==="up").length,e=this.targets.filter(a=>a.availability==="down").length,i=this.alerts.filter(a=>a.delivery==="pending").length,s=js(this.targets,this.live),n=["overview","alerts","cluster"],r=this.targets.filter(a=>`${a.name} ${a.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(a=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?a.paused:a.availability===this.statusFilter).sort((a,o)=>this.sort==="status"&&a.availability.localeCompare(o.availability)||a.name.localeCompare(o.name));return this.authReady&&!this.setupMode&&!this.session?c`${zs(this.saving,this.error,{login:a=>{this.login(a)}})}${Ae()}`:this.setupMode&&this.setup?c`
        <main class="shell setup-shell">
          <header>
            <div class="brand">
              <img src="/favicon.svg" alt="" />
              <div><div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"up":""}"></i>${this.live?"ready":"connecting"}</div></div><span>Distributed service monitoring</span></div>
            </div>
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${_t[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:g}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>${Ae()}`:c`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${s.tone}"></i>${s.label}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${n.map(a=>c`<a class=${this.activeSection===a?"active":""} href=${Ge[a]} @click=${o=>this.navigate(o,a)}>${a[0].toUpperCase()}${a.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${_t[this.theme]} aria-hidden="true"></iconify-icon></button>
            <span class="meta">${this.session?.username}</span>
            <button class="button secondary" @click=${()=>{this.logout()}}>Sign out</button>
          </div>
        </header>
        ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:g}
        ${this.setup?.warning&&!this.warningDismissed?c`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:g}
        ${this.activeSection==="overview"?this.renderOverview(r,t,e,i):this.activeSection==="alerts"?Fs(this.alerts,this.transitions,this.channels,{search:this.alertSearch,delivery:this.alertDeliveryFilter,kind:this.alertKindFilter,acknowledged:this.alertAcknowledgedFilter},this.saving,{create:()=>this.openChannelDialog(),edit:a=>this.openChannelDialog(a),remove:a=>{this.deleteResource("channels",a.id,a.name)},setDefault:(a,o)=>{this.setChannelDefault(a,o)},acknowledge:a=>{this.acknowledgeAlert(a)},retry:a=>{this.retryAlert(a)},setSearch:a=>this.alertSearch=a,setDelivery:a=>this.alertDeliveryFilter=a,setKind:a=>this.alertKindFilter=a,setAcknowledged:a=>this.alertAcknowledgedFilter=a}):this.renderClusterPage()}
      </main>${Ae()}
      ${Ys(this.channels,this.secrets,this.saving,{backdrop:a=>this.dismissOnBackdrop(a),close:()=>this.closeTargetDialog(),create:a=>{this.createTarget(a)}})}
      ${this.selected?Zs(this.selected,this.targetHistory,this.historyLoading,this.saving,this.detailDirty,this.cluster?.members??[],this.channels,this.secrets,{backdrop:a=>this.dismissOnBackdrop(a),close:()=>this.closeDetailDialog(),update:a=>{this.updateTarget(a)},changed:a=>this.updateDetailDirty(a),redirects:a=>this.toggleMaxRedirects(a),delete:()=>{this.deleteTarget()},pause:a=>{this.setPaused(a)}}):g}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="secret-title">Add secret</h2><p>Create an encrypted, write-only value to reference from Target requests or webhook headers through the HTTP API.</p></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">${this.editingChannel?"Edit channel":"Add channel"}</h2><p>${this.editingChannel?"Update this destination without changing its Channel type.":"Send transitions through Telegram, SMTP, or a generic webhook."}</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" .value=${this.channelKind} ?disabled=${this.editingChannel!==void 0} @change=${a=>{this.channelKind=a.target.value,this.channelTestMessage=""}}><option value="webhook">Webhook</option><option value="telegram">Telegram</option><option value="smtp">SMTP email</option></select></label>
          <label>Name<input name="name" placeholder="On-call" .value=${this.editingChannel?.name??""} required /></label>
          ${Ks(this.channelKind,this.editingChannel)}
          <label class="switch"><span>Default channel</span><input name="default" type="checkbox" role="switch" .checked=${this.editingChannel?.default??!1} /></label>
          <div class="dialog-actions">${this.channelTestMessage?c`<span class="meta" role="status" style="margin-right:auto">${this.channelTestMessage}</span>`:g}<button class="button secondary" type="button" @click=${()=>{this.editingChannel=void 0,this.closeDialog("channel-dialog")}}>Cancel</button>${this.editingChannel?g:c`<button class="button secondary" type="button" aria-busy=${this.testingChannel} ?disabled=${this.testingChannel||this.saving} @click=${this.testChannel}>${this.testingChannel?"Sending…":"Send test"}</button>`}<button class="button" type="submit" ?disabled=${this.saving||this.testingChannel}>${this.editingChannel?"Save changes":"Create channel"}</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create Join Token</h2><p>Choose how many days the token remains valid and whether it can be reused.</p></div>
        <form @submit=${this.createJoinToken}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required /></label>
          <label class="switch"><span>Unlimited uses</span><input type="checkbox" role="switch" .checked=${this.unlimitedUses} @change=${a=>this.unlimitedUses=a.target.checked} /></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join Token Created</h2><p>This command contains Cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderOverview(t,e,i,s){const n=this.targets.filter(o=>this.selectedIds.has(o.id)),r=n.some(o=>!o.paused),a=n.some(o=>o.paused);return c`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="overview-top">
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${s}</strong></div>
          <div class="metric"><span>Up</span><strong>${e}</strong></div>
          <div class=${`metric down ${i?"active":""}`}><span>Down</span><strong>${i}</strong></div>
        </section>
        <section class="panel" aria-label="Secrets">
          <div class="panel-head"><div class="title-with-help"><h2>Secrets</h2>${ge("secrets-help","About reusable Secrets","Reusable Secrets are encrypted and write-only. Reference their IDs in Target headers or bodies and webhook headers through the HTTP API.")}</div><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length?this.secrets.map(o=>c`<div class="resource"><div><strong>${o.name}</strong><code>${o.id}</code></div><button class="button danger icon-button" aria-label=${`Delete secret ${o.name}`} title=${`Delete ${o.name}`} @click=${()=>this.deleteResource("secrets",o.id,o.name)}><iconify-icon .icon=${de} aria-hidden="true"></iconify-icon></button></div>`):c`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${o=>this.search=o.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${o=>this.statusFilter=o.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${o=>this.sort=o.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?c`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${It} aria-hidden="true"></iconify-icon></button>${r?c`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${Pt} aria-hidden="true"></iconify-icon></button>`:g}${a?c`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${Dt} aria-hidden="true"></iconify-icon></button>`:g}<button class="button danger icon-button" aria-label="Delete selected" title="Delete selected" @click=${this.bulkDelete}><iconify-icon .icon=${de} aria-hidden="true"></iconify-icon></button></div></div>`:g}
        ${t.length?t.map(o=>this.renderTarget(o)):c`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
    `}renderClusterMember(t){return c`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.raft_url} · ${t.active_assignments} active assignments</code>
        </div>
        <div class="actions">
          ${t.local?c`<span class="badge">This node</span>`:g}
          ${t.leader?c`<span class="badge">Leader</span>`:g}
          ${t.draining?c`<span class="badge">Draining</span>`:g}
          ${t.local?g:c`
                <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.setNodeDrain(t,!t.draining)}>${t.draining?"Cancel drain":"Drain"}</button>
                ${t.draining&&t.active_assignments===0?c`<button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!1)}>Remove</button>`:g}
                <button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!0)}>Replace failed</button>
              `}
        </div>
      </div>
    `}renderClusterPage(){return c`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><div><h2>Nodes</h2><p class="meta">Drain healthy Nodes before removal. Replace failed Nodes only after confirming the old process is permanently stopped.</p></div><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(t=>this.renderClusterMember(t))}
        ${this.cluster?.members.length?g:c`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(t=>c`
              <div class="resource">
                <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
              </div>
            `):c`<div class="empty">No Join Tokens.</div>`}
      </section>
      </div>
      ${Hs(this.identities,this.apiTokens,this.session?.identity_id,this.newApiToken,this.saving,{login:t=>{this.login(t)},logout:()=>{this.logout()},createIdentity:t=>{this.createIdentity(t)},updateIdentity:(t,e)=>{this.updateIdentity(t,e)},deleteIdentity:t=>{this.deleteIdentity(t)},createApiToken:t=>{this.createApiToken(t)},revokeApiToken:t=>{this.revokeApiToken(t)},dismissToken:()=>this.newApiToken=""})}
    `}renderTarget(t){const e=t.kind==="node",i=t.kind==="http",s=t.latest_evaluation,n=t.history.slice(0,16).reverse(),r=Math.max(1,...n.map(o=>o.latency_ms)),a=t.paused?"paused":t.availability==="down"?"down":t.consecutive_failures>0?"suspicious":t.availability;return c`
      <div class="target-wrap">
        ${e?c`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} disabled />`:c`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${o=>this.toggleSelected(t.id,o.target.checked)} />`}
        <button class=${`target ${e?"node-target":""}`} aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${a}" aria-label=${a}></i>
          <div>
            <div class="target-title"><h3>${t.name}</h3><span class="badge">${e?"Node":t.kind.toUpperCase()}</span></div>
            <div class="meta">${t.paused?"Paused · ":""}${i||e?`${t.method} · `:""}${t.url} · every ${t.interval_seconds}s${e?"":` · ${t.locations} ${t.locations===1?"location":"locations"}`}</div>
          </div>
          <div class="target-side">
            ${n.length?c`<div class="mini-chart" aria-hidden="true">${n.map(o=>c`<i class="mini-bar ${o.succeeded?"up":"down"}" style=${`height: ${Math.max(12,o.latency_ms/r*100)}%`}></i>`)}</div>`:g}
            <div class="latency">
              <strong>${s?`${s.latency_ms} ms`:"—"}</strong>
              <span>${s?i?s.status_code??"network error":s.succeeded?"reachable":"unreachable":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};Re.styles=fe`
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
      display: flex; flex-direction: column;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { flex: 1 0 auto; width: 100%; max-width: 1200px; margin: auto; padding: 28px 24px 48px; }
    .setup-shell { display: grid; grid-template-rows: auto minmax(0, 1fr); padding-top: 20px; padding-bottom: 20px; }
    .setup-shell header { margin-bottom: 18px; } .setup-shell upgrid-setup { align-self: center; }
    header { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); align-items: center; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    header > .brand { justify-self: start; }
    header > nav { justify-self: center; }
    header > .actions { justify-self: end; }
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
    .dot.up { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .dot.degraded { background: var(--amber); box-shadow: 0 0 10px var(--amber); }
    .dot.down { background: var(--red); box-shadow: 0 0 10px var(--red); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 30px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 44px; height: 44px; min-height: 44px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    ${Bs}
    .auth-panel { width: min(440px, 100%); margin: auto; }
    .access-panels { margin-top: 18px; }
    .access-resource { align-items: end; }
    .access-form { display: grid; flex: 1; grid-template-columns: 1fr 1fr auto; align-items: end; gap: 9px; }
    .compact-form { border-top: 1px solid var(--divider); }
    .compact-form h3 { margin: 0; }
    .token-value { margin: 14px; overflow-wrap: anywhere; }
    .token-value code { display: block; margin: 8px 0; }
    .overview-top { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-bottom: 18px; }
    .page-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .summary { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .metric.down.active span, .metric.down.active strong { color: var(--red); }
    .panel { border-radius: 16px; overflow: hidden; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .badge.up { border-color: var(--green); color: var(--green); }
    .badge.down { border-color: var(--red); color: var(--red); }
    .transition-main { display: flex; align-items: center; gap: 12px; }
    .channel-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .channel-summary { min-width: 0; }
    .channel-title, .channel-actions { display: flex; align-items: center; gap: 10px; }
    .channel-summary code { display: block; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .channel-actions .switch span { font-size: 12px; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .alert-history { margin-bottom: 20px; }
    .alert-filters { display: grid; grid-template-columns: minmax(180px, 1fr) repeat(3, minmax(120px, auto)); gap: 10px; padding: 14px 20px; border-bottom: 1px solid var(--line); }
    .alert-filters label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    .alert-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .alert-summary { display: grid; min-width: 0; gap: 4px; }
    .alert-summary code, .alert-summary .meta { font-size: 11px; }
    .alert-summary .meta { color: var(--muted); white-space: normal; }
    .alert-actions { display: flex; align-items: center; gap: 8px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 24px; height: 24px; accent-color: var(--green); }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .node-target { cursor: pointer; }
    .state { width: 10px; height: 10px; border-radius: 50%; color: var(--amber); background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .target-title { display: flex; align-items: center; gap: 8px; margin-bottom: 3px; }
    .target-title h3 { margin: 0; }
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
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    [hidden] { display: none !important; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, a:focus-visible, .target:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
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
    .check { display: flex; align-items: center; gap: 8px; } .check input { width: 18px; min-height: 18px; height: 18px; flex: none; }
    .channel-fields { display: grid; gap: 10px; margin: 8px 0 0; border: 0; padding: 0; }
    .channel-fields legend { display: flex; width: 100%; align-items: center; gap: 12px; margin: 0 0 4px; padding: 0; color: var(--text); font-size: 14px; font-weight: 400; text-align: center; }
    .channel-fields legend::before, .channel-fields legend::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    form .badge { font-size: 12px; }
    .channel-options { display: grid; gap: 6px; }
    .channel-options .check { min-height: 36px; border-radius: 8px; padding: 5px 8px; background: var(--panel-2); }
    .channel-options .badge { margin-left: auto; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .switch input { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch input::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch input:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch input:checked::after { background: var(--button-text); transform: translateX(18px); }
    footer { display: flex; flex: 0 0 auto; width: calc(100% - 48px); max-width: 1152px; flex-direction: column; align-items: center; justify-content: center; gap: 8px; margin: 0 auto; border-top: 1px solid var(--line); padding: 20px 0 24px; color: var(--muted); font-size: 12px; }
    .footer-links, .footer-powered { display: flex; align-items: center; justify-content: center; flex-wrap: wrap; gap: 10px; text-align: center; }
    footer a { display: inline-flex; align-items: center; gap: 5px; border-radius: 4px; color: var(--green); text-decoration: underline; text-underline-offset: 3px; transition: color 160ms ease; }
    footer a:hover { color: var(--text); }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .history-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
    .history-summary div { padding: 12px; border: 1px solid var(--line); border-radius: 9px; background: var(--input-bg); }
    .history-summary span { display: block; color: var(--muted); font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
    .history-summary strong { display: block; margin-top: 5px; font-size: 18px; font-weight: 560; }
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
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select, .help-tooltip-trigger, .help-tooltip { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      header { grid-template-columns: minmax(0, 1fr) auto; row-gap: 16px; }
      header > nav { display: flex; grid-column: 1 / -1; grid-row: 2; justify-self: center; }
      .overview-top { grid-template-columns: 1fr; }
      .page-columns { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target-wrap { align-items: start; padding-left: 14px; } .select-target { align-self: start; margin-top: 0; } .target { grid-template-columns: auto minmax(0, 1fr); gap: 10px; padding: 12px 14px 12px 10px; }
      .target-side { grid-column: 2; display: grid; grid-template-columns: minmax(88px, 1fr) auto; width: 100%; gap: 18px; margin-top: 4px; } .target > .state { align-self: start; margin-top: 5px; } .mini-chart { width: 100%; max-width: 140px; height: 28px; }
      .latency { min-width: 72px; text-align: right; }
      .channel-resource { grid-template-columns: 1fr; }
      .alert-filters { grid-template-columns: 1fr 1fr; }
      .alert-resource { grid-template-columns: 1fr; }
      .alert-actions { margin-top: 8px; }
      .channel-actions { justify-content: space-between; margin-top: 10px; }
      .access-form { grid-template-columns: 1fr; }
    }
  `;Re=en([He("upgrid-app")],Re);
