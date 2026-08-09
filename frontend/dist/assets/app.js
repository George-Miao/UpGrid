(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const n of document.querySelectorAll('link[rel="modulepreload"]'))s(n);new MutationObserver(n=>{for(const r of n)if(r.type==="childList")for(const o of r.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&s(o)}).observe(document,{childList:!0,subtree:!0});function i(n){const r={};return n.integrity&&(r.integrity=n.integrity),n.referrerPolicy&&(r.referrerPolicy=n.referrerPolicy),n.crossOrigin==="use-credentials"?r.credentials="include":n.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function s(n){if(n.ep)return;n.ep=!0;const r=i(n);fetch(n.href,r)}})();const te=globalThis,Ae=te.ShadowRoot&&(te.ShadyCSS===void 0||te.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Te=Symbol(),Re=new WeakMap;let ut=class{constructor(e,i,s){if(this._$cssResult$=!0,s!==Te)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=i}get styleSheet(){let e=this.o;const i=this.t;if(Ae&&e===void 0){const s=i!==void 0&&i.length===1;s&&(e=Re.get(i)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),s&&Re.set(i,e))}return e}toString(){return this.cssText}};const Ft=t=>new ut(typeof t=="string"?t:t+"",void 0,Te),ht=(t,...e)=>{const i=t.length===1?t[0]:e.reduce((s,n,r)=>s+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(n)+t[r+1],t[0]);return new ut(i,t,Te)},qt=(t,e)=>{if(Ae)t.adoptedStyleSheets=e.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of e){const s=document.createElement("style"),n=te.litNonce;n!==void 0&&s.setAttribute("nonce",n),s.textContent=i.cssText,t.appendChild(s)}},Le=Ae?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let i="";for(const s of e.cssRules)i+=s.cssText;return Ft(i)})(t):t;const{is:zt,defineProperty:Ht,getOwnPropertyDescriptor:Jt,getOwnPropertyNames:Bt,getOwnPropertySymbols:Vt,getPrototypeOf:Kt}=Object,de=globalThis,Ue=de.trustedTypes,Qt=Ue?Ue.emptyScript:"",Wt=de.reactiveElementPolyfillSupport,J=(t,e)=>t,re={toAttribute(t,e){switch(e){case Boolean:t=t?Qt:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let i=t;switch(e){case Boolean:i=t!==null;break;case Number:i=t===null?null:Number(t);break;case Object:case Array:try{i=JSON.parse(t)}catch{i=null}}return i}},Ee=(t,e)=>!zt(t,e),Fe={attribute:!0,type:String,converter:re,reflect:!1,useDefault:!1,hasChanged:Ee};Symbol.metadata??=Symbol("metadata"),de.litPropertyMetadata??=new WeakMap;let N=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,i=Fe){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(e,i),!i.noAccessor){const s=Symbol(),n=this.getPropertyDescriptor(e,s,i);n!==void 0&&Ht(this.prototype,e,n)}}static getPropertyDescriptor(e,i,s){const{get:n,set:r}=Jt(this.prototype,e)??{get(){return this[i]},set(o){this[i]=o}};return{get:n,set(o){const a=n?.call(this);r?.call(this,o),this.requestUpdate(e,a,s)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??Fe}static _$Ei(){if(this.hasOwnProperty(J("elementProperties")))return;const e=Kt(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(J("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(J("properties"))){const i=this.properties,s=[...Bt(i),...Vt(i)];for(const n of s)this.createProperty(n,i[n])}const e=this[Symbol.metadata];if(e!==null){const i=litPropertyMetadata.get(e);if(i!==void 0)for(const[s,n]of i)this.elementProperties.set(s,n)}this._$Eh=new Map;for(const[i,s]of this.elementProperties){const n=this._$Eu(i,s);n!==void 0&&this._$Eh.set(n,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const i=[];if(Array.isArray(e)){const s=new Set(e.flat(1/0).reverse());for(const n of s)i.unshift(Le(n))}else e!==void 0&&i.push(Le(e));return i}static _$Eu(e,i){const s=i.attribute;return s===!1?void 0:typeof s=="string"?s:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,i=this.constructor.elementProperties;for(const s of i.keys())this.hasOwnProperty(s)&&(e.set(s,this[s]),delete this[s]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return qt(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,i,s){this._$AK(e,s)}_$ET(e,i){const s=this.constructor.elementProperties.get(e),n=this.constructor._$Eu(e,s);if(n!==void 0&&s.reflect===!0){const r=(s.converter?.toAttribute!==void 0?s.converter:re).toAttribute(i,s.type);this._$Em=e,r==null?this.removeAttribute(n):this.setAttribute(n,r),this._$Em=null}}_$AK(e,i){const s=this.constructor,n=s._$Eh.get(e);if(n!==void 0&&this._$Em!==n){const r=s.getPropertyOptions(n),o=typeof r.converter=="function"?{fromAttribute:r.converter}:r.converter?.fromAttribute!==void 0?r.converter:re;this._$Em=n;const a=o.fromAttribute(i,r.type);this[n]=a??this._$Ej?.get(n)??a,this._$Em=null}}requestUpdate(e,i,s,n=!1,r){if(e!==void 0){const o=this.constructor;if(n===!1&&(r=this[e]),s??=o.getPropertyOptions(e),!((s.hasChanged??Ee)(r,i)||s.useDefault&&s.reflect&&r===this._$Ej?.get(e)&&!this.hasAttribute(o._$Eu(e,s))))return;this.C(e,i,s)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,i,{useDefault:s,reflect:n,wrapped:r},o){s&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,o??i??this[e]),r!==!0||o!==void 0)||(this._$AL.has(e)||(this.hasUpdated||s||(i=void 0),this._$AL.set(e,i)),n===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[n,r]of this._$Ep)this[n]=r;this._$Ep=void 0}const s=this.constructor.elementProperties;if(s.size>0)for(const[n,r]of s){const{wrapped:o}=r,a=this[n];o!==!0||this._$AL.has(n)||a===void 0||this.C(n,void 0,r,a)}}let e=!1;const i=this._$AL;try{e=this.shouldUpdate(i),e?(this.willUpdate(i),this._$EO?.forEach(s=>s.hostUpdate?.()),this.update(i)):this._$EM()}catch(s){throw e=!1,this._$EM(),s}e&&this._$AE(i)}willUpdate(e){}_$AE(e){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(e){}firstUpdated(e){}};N.elementStyles=[],N.shadowRootOptions={mode:"open"},N[J("elementProperties")]=new Map,N[J("finalized")]=new Map,Wt?.({ReactiveElement:N}),(de.reactiveElementVersions??=[]).push("2.1.2");const Pe=globalThis,qe=t=>t,oe=Pe.trustedTypes,ze=oe?oe.createPolicy("lit-html",{createHTML:t=>t}):void 0,pt="$lit$",A=`lit$${Math.random().toFixed(9).slice(2)}$`,ft="?"+A,Gt=`<${ft}>`,j=document,V=()=>j.createComment(""),K=t=>t===null||typeof t!="object"&&typeof t!="function",Ie=Array.isArray,Yt=t=>Ie(t)||typeof t?.[Symbol.iterator]=="function",me=`[ 	
\f\r]`,q=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,He=/-->/g,Je=/>/g,O=RegExp(`>|${me}(?:([^\\s"'>=/]+)(${me}*=${me}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),Be=/'/g,Ve=/"/g,gt=/^(?:script|style|textarea|title)$/i,Zt=t=>(e,...i)=>({_$litType$:t,strings:e,values:i}),h=Zt(1),R=Symbol.for("lit-noChange"),f=Symbol.for("lit-nothing"),Ke=new WeakMap,D=j.createTreeWalker(j,129);function mt(t,e){if(!Ie(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return ze!==void 0?ze.createHTML(e):e}const Xt=(t,e)=>{const i=t.length-1,s=[];let n,r=e===2?"<svg>":e===3?"<math>":"",o=q;for(let a=0;a<i;a++){const l=t[a];let c,d,u=-1,p=0;for(;p<l.length&&(o.lastIndex=p,d=o.exec(l),d!==null);)p=o.lastIndex,o===q?d[1]==="!--"?o=He:d[1]!==void 0?o=Je:d[2]!==void 0?(gt.test(d[2])&&(n=RegExp("</"+d[2],"g")),o=O):d[3]!==void 0&&(o=O):o===O?d[0]===">"?(o=n??q,u=-1):d[1]===void 0?u=-2:(u=o.lastIndex-d[2].length,c=d[1],o=d[3]===void 0?O:d[3]==='"'?Ve:Be):o===Ve||o===Be?o=O:o===He||o===Je?o=q:(o=O,n=void 0);const x=o===O&&t[a+1].startsWith("/>")?" ":"";r+=o===q?l+Gt:u>=0?(s.push(c),l.slice(0,u)+pt+l.slice(u)+A+x):l+A+(u===-2?a:x)}return[mt(t,r+(t[i]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),s]};class Q{constructor({strings:e,_$litType$:i},s){let n;this.parts=[];let r=0,o=0;const a=e.length-1,l=this.parts,[c,d]=Xt(e,i);if(this.el=Q.createElement(c,s),D.currentNode=this.el.content,i===2||i===3){const u=this.el.content.firstChild;u.replaceWith(...u.childNodes)}for(;(n=D.nextNode())!==null&&l.length<a;){if(n.nodeType===1){if(n.hasAttributes())for(const u of n.getAttributeNames())if(u.endsWith(pt)){const p=d[o++],x=n.getAttribute(u).split(A),w=/([.?@])?(.*)/.exec(p);l.push({type:1,index:r,name:w[2],strings:x,ctor:w[1]==="."?ti:w[1]==="?"?ii:w[1]==="@"?si:ue}),n.removeAttribute(u)}else u.startsWith(A)&&(l.push({type:6,index:r}),n.removeAttribute(u));if(gt.test(n.tagName)){const u=n.textContent.split(A),p=u.length-1;if(p>0){n.textContent=oe?oe.emptyScript:"";for(let x=0;x<p;x++)n.append(u[x],V()),D.nextNode(),l.push({type:2,index:++r});n.append(u[p],V())}}}else if(n.nodeType===8)if(n.data===ft)l.push({type:2,index:r});else{let u=-1;for(;(u=n.data.indexOf(A,u+1))!==-1;)l.push({type:7,index:r}),u+=A.length-1}r++}}static createElement(e,i){const s=j.createElement("template");return s.innerHTML=e,s}}function L(t,e,i=t,s){if(e===R)return e;let n=s!==void 0?i._$Co?.[s]:i._$Cl;const r=K(e)?void 0:e._$litDirective$;return n?.constructor!==r&&(n?._$AO?.(!1),r===void 0?n=void 0:(n=new r(t),n._$AT(t,i,s)),s!==void 0?(i._$Co??=[])[s]=n:i._$Cl=n),n!==void 0&&(e=L(t,n._$AS(t,e.values),n,s)),e}class ei{constructor(e,i){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:i},parts:s}=this._$AD,n=(e?.creationScope??j).importNode(i,!0);D.currentNode=n;let r=D.nextNode(),o=0,a=0,l=s[0];for(;l!==void 0;){if(o===l.index){let c;l.type===2?c=new Y(r,r.nextSibling,this,e):l.type===1?c=new l.ctor(r,l.name,l.strings,this,e):l.type===6&&(c=new ni(r,this,e)),this._$AV.push(c),l=s[++a]}o!==l?.index&&(r=D.nextNode(),o++)}return D.currentNode=j,n}p(e){let i=0;for(const s of this._$AV)s!==void 0&&(s.strings!==void 0?(s._$AI(e,s,i),i+=s.strings.length-2):s._$AI(e[i])),i++}}class Y{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,i,s,n){this.type=2,this._$AH=f,this._$AN=void 0,this._$AA=e,this._$AB=i,this._$AM=s,this.options=n,this._$Cv=n?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&e?.nodeType===11&&(e=i.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,i=this){e=L(this,e,i),K(e)?e===f||e==null||e===""?(this._$AH!==f&&this._$AR(),this._$AH=f):e!==this._$AH&&e!==R&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):Yt(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==f&&K(this._$AH)?this._$AA.nextSibling.data=e:this.T(j.createTextNode(e)),this._$AH=e}$(e){const{values:i,_$litType$:s}=e,n=typeof s=="number"?this._$AC(e):(s.el===void 0&&(s.el=Q.createElement(mt(s.h,s.h[0]),this.options)),s);if(this._$AH?._$AD===n)this._$AH.p(i);else{const r=new ei(n,this),o=r.u(this.options);r.p(i),this.T(o),this._$AH=r}}_$AC(e){let i=Ke.get(e.strings);return i===void 0&&Ke.set(e.strings,i=new Q(e)),i}k(e){Ie(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let s,n=0;for(const r of e)n===i.length?i.push(s=new Y(this.O(V()),this.O(V()),this,this.options)):s=i[n],s._$AI(r),n++;n<i.length&&(this._$AR(s&&s._$AB.nextSibling,n),i.length=n)}_$AR(e=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);e!==this._$AB;){const s=qe(e).nextSibling;qe(e).remove(),e=s}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class ue{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,i,s,n,r){this.type=1,this._$AH=f,this._$AN=void 0,this.element=e,this.name=i,this._$AM=n,this.options=r,s.length>2||s[0]!==""||s[1]!==""?(this._$AH=Array(s.length-1).fill(new String),this.strings=s):this._$AH=f}_$AI(e,i=this,s,n){const r=this.strings;let o=!1;if(r===void 0)e=L(this,e,i,0),o=!K(e)||e!==this._$AH&&e!==R,o&&(this._$AH=e);else{const a=e;let l,c;for(e=r[0],l=0;l<r.length-1;l++)c=L(this,a[s+l],i,l),c===R&&(c=this._$AH[l]),o||=!K(c)||c!==this._$AH[l],c===f?e=f:e!==f&&(e+=(c??"")+r[l+1]),this._$AH[l]=c}o&&!n&&this.j(e)}j(e){e===f?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class ti extends ue{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===f?void 0:e}}class ii extends ue{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==f)}}class si extends ue{constructor(e,i,s,n,r){super(e,i,s,n,r),this.type=5}_$AI(e,i=this){if((e=L(this,e,i,0)??f)===R)return;const s=this._$AH,n=e===f&&s!==f||e.capture!==s.capture||e.once!==s.once||e.passive!==s.passive,r=e!==f&&(s===f||n);n&&this.element.removeEventListener(this.name,this,s),r&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class ni{constructor(e,i,s){this.element=e,this.type=6,this._$AN=void 0,this._$AM=i,this.options=s}get _$AU(){return this._$AM._$AU}_$AI(e){L(this,e)}}const ri=Pe.litHtmlPolyfillSupport;ri?.(Q,Y),(Pe.litHtmlVersions??=[]).push("3.3.3");const oi=(t,e,i)=>{const s=i?.renderBefore??e;let n=s._$litPart$;if(n===void 0){const r=i?.renderBefore??null;s._$litPart$=n=new Y(e.insertBefore(V(),r),r,void 0,i??{})}return n._$AI(t),n};const Oe=globalThis;class M extends N{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=oi(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return R}}M._$litElement$=!0,M.finalized=!0,Oe.litElementHydrateSupport?.({LitElement:M});const ai=Oe.litElementPolyfillSupport;ai?.({LitElement:M});(Oe.litElementVersions??=[]).push("4.2.2");const bt=t=>(e,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const li={attribute:!0,type:String,converter:re,reflect:!1,hasChanged:Ee},ci=(t=li,e,i)=>{const{kind:s,metadata:n}=i;let r=globalThis.litPropertyMetadata.get(n);if(r===void 0&&globalThis.litPropertyMetadata.set(n,r=new Map),s==="setter"&&((t=Object.create(t)).wrapped=!0),r.set(i.name,t),s==="accessor"){const{name:o}=i;return{set(a){const l=e.get.call(this);e.set.call(this,a),this.requestUpdate(o,l,t,!0,a)},init(a){return a!==void 0&&this.C(o,void 0,t,a),a}}}if(s==="setter"){const{name:o}=i;return function(a){const l=this[o];e.call(this,a),this.requestUpdate(o,l,t,!0,a)}}throw Error("Unsupported decorator location: "+s)};function vt(t){return(e,i)=>typeof i=="object"?ci(t,e,i):((s,n,r)=>{const o=n.hasOwnProperty(r);return n.constructor.createProperty(r,s),o?Object.getOwnPropertyDescriptor(n,r):void 0})(t,e,i)}function g(t){return vt({...t,state:!0,attribute:!1})}const yt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},xt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},$t={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const wt=Object.freeze({left:0,top:0,width:16,height:16}),ae=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),Z=Object.freeze({...wt,...ae}),xe=Object.freeze({...Z,body:"",hidden:!1}),di=Object.freeze({width:null,height:null}),_t=Object.freeze({...di,...ae});function ui(t,e=0){const i=t.replace(/^-?[0-9.]*/,"");function s(n){for(;n<0;)n+=4;return n%4}if(i===""){const n=parseInt(t);return isNaN(n)?0:s(n)}else if(i!==t){let n=0;switch(i){case"%":n=25;break;case"deg":n=90}if(n){let r=parseFloat(t.slice(0,t.length-i.length));return isNaN(r)?0:(r=r/n,r%1===0?s(r):0)}}return e}const hi=/[\s,]+/;function pi(t,e){e.split(hi).forEach(i=>{switch(i.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const kt={..._t,preserveAspectRatio:""};function Qe(t){const e={...kt},i=(s,n)=>t.getAttribute(s)||n;return e.width=i("width",null),e.height=i("height",null),e.rotate=ui(i("rotate","")),pi(e,i("flip","")),e.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),e}function fi(t,e){for(const i in kt)if(t[i]!==e[i])return!0;return!1}const St=/^[a-z0-9]+(-[a-z0-9]+)*$/,X=(t,e,i,s="")=>{const n=t.split(":");if(t.slice(0,1)==="@"){if(n.length<2||n.length>3)return null;s=n.shift().slice(1)}if(n.length>3||!n.length)return null;if(n.length>1){const a=n.pop(),l=n.pop(),c={provider:n.length>0?n[0]:s,prefix:l,name:a};return e&&!ie(c)?null:c}const r=n[0],o=r.split("-");if(o.length>1){const a={provider:s,prefix:o.shift(),name:o.join("-")};return e&&!ie(a)?null:a}if(i&&s===""){const a={provider:s,prefix:"",name:r};return e&&!ie(a,i)?null:a}return null},ie=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function gi(t,e){const i=t.icons,s=t.aliases||Object.create(null),n=Object.create(null);function r(o){if(i[o])return n[o]=[];if(!(o in n)){n[o]=null;const a=s[o]&&s[o].parent,l=a&&r(a);l&&(n[o]=[a].concat(l))}return n[o]}return Object.keys(i).concat(Object.keys(s)).forEach(r),n}function mi(t,e){const i={};!t.hFlip!=!e.hFlip&&(i.hFlip=!0),!t.vFlip!=!e.vFlip&&(i.vFlip=!0);const s=((t.rotate||0)+(e.rotate||0))%4;return s&&(i.rotate=s),i}function We(t,e){const i=mi(t,e);for(const s in xe)s in ae?s in t&&!(s in i)&&(i[s]=ae[s]):s in e?i[s]=e[s]:s in t&&(i[s]=t[s]);return i}function bi(t,e,i){const s=t.icons,n=t.aliases||Object.create(null);let r={};function o(a){r=We(s[a]||n[a],r)}return o(e),i.forEach(o),We(t,r)}function Ct(t,e){const i=[];if(typeof t!="object"||typeof t.icons!="object")return i;t.not_found instanceof Array&&t.not_found.forEach(n=>{e(n,null),i.push(n)});const s=gi(t);for(const n in s){const r=s[n];r&&(e(n,bi(t,n,r)),i.push(n))}return i}const vi={provider:"",aliases:{},not_found:{},...wt};function be(t,e){for(const i in e)if(i in t&&typeof t[i]!=typeof e[i])return!1;return!0}function At(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!be(t,vi))return null;const i=e.icons;for(const n in i){const r=i[n];if(!n||typeof r.body!="string"||!be(r,xe))return null}const s=e.aliases||Object.create(null);for(const n in s){const r=s[n],o=r.parent;if(!n||typeof o!="string"||!i[o]&&!s[o]||!be(r,xe))return null}return e}const le=Object.create(null);function yi(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function C(t,e){const i=le[t]||(le[t]=Object.create(null));return i[e]||(i[e]=yi(t,e))}function Tt(t,e){return At(e)?Ct(e,(i,s)=>{s?t.icons[i]=s:t.missing.add(i)}):[]}function xi(t,e,i){try{if(typeof i.body=="string")return t.icons[e]={...i},!0}catch{}return!1}function $i(t,e){let i=[];return(typeof t=="string"?[t]:Object.keys(le)).forEach(s=>{(typeof s=="string"&&typeof e=="string"?[e]:Object.keys(le[s]||{})).forEach(n=>{const r=C(s,n);i=i.concat(Object.keys(r.icons).map(o=>(s!==""?"@"+s+":":"")+n+":"+o))})}),i}let W=!1;function Et(t){return typeof t=="boolean"&&(W=t),W}function G(t){const e=typeof t=="string"?X(t,!0,W):t;if(e){const i=C(e.provider,e.prefix),s=e.name;return i.icons[s]||(i.missing.has(s)?null:void 0)}}function Pt(t,e){const i=X(t,!0,W);if(!i)return!1;const s=C(i.provider,i.prefix);return e?xi(s,i.name,e):(s.missing.add(i.name),!0)}function Ge(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),W&&!e&&!t.prefix){let s=!1;return At(t)&&(t.prefix="",Ct(t,(n,r)=>{Pt(n,r)&&(s=!0)})),s}const i=t.prefix;return ie({prefix:i,name:"a"})?!!Tt(C(e,i),t):!1}function wi(t){return!!G(t)}function _i(t){const e=G(t);return e&&{...Z,...e}}function It(t,e){t.forEach(i=>{const s=i.loaderCallbacks;s&&(i.loaderCallbacks=s.filter(n=>n.id!==e))})}function ki(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let i=!1;const s=t.provider,n=t.prefix;e.forEach(r=>{const o=r.icons,a=o.pending.length;o.pending=o.pending.filter(l=>{if(l.prefix!==n)return!0;const c=l.name;if(t.icons[c])o.loaded.push({provider:s,prefix:n,name:c});else if(t.missing.has(c))o.missing.push({provider:s,prefix:n,name:c});else return i=!0,!0;return!1}),o.pending.length!==a&&(i||It([t],r.id),r.callback(o.loaded.slice(0),o.missing.slice(0),o.pending.slice(0),r.abort))})}))}let Si=0;function Ci(t,e,i){const s=Si++,n=It.bind(null,i,s);if(!e.pending.length)return n;const r={id:s,icons:e,callback:t,abort:n};return i.forEach(o=>{(o.loaderCallbacks||(o.loaderCallbacks=[])).push(r)}),n}function Ai(t){const e={loaded:[],missing:[],pending:[]},i=Object.create(null);t.sort((n,r)=>n.provider!==r.provider?n.provider.localeCompare(r.provider):n.prefix!==r.prefix?n.prefix.localeCompare(r.prefix):n.name.localeCompare(r.name));let s={provider:"",prefix:"",name:""};return t.forEach(n=>{if(s.name===n.name&&s.prefix===n.prefix&&s.provider===n.provider)return;s=n;const r=n.provider,o=n.prefix,a=n.name,l=i[r]||(i[r]=Object.create(null)),c=l[o]||(l[o]=C(r,o));let d;a in c.icons?d=e.loaded:o===""||c.missing.has(a)?d=e.missing:d=e.pending;const u={provider:r,prefix:o,name:a};d.push(u)}),e}const $e=Object.create(null);function Ye(t,e){$e[t]=e}function we(t){return $e[t]||$e[""]}function Ti(t,e=!0,i=!1){const s=[];return t.forEach(n=>{const r=typeof n=="string"?X(n,e,i):n;r&&s.push(r)}),s}function De(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const he=Object.create(null),z=["https://api.simplesvg.com","https://api.unisvg.com"],se=[];for(;z.length>0;)z.length===1||Math.random()>.5?se.push(z.shift()):se.push(z.pop());he[""]=De({resources:["https://api.iconify.design"].concat(se)});function Ze(t,e){const i=De(e);return i===null?!1:(he[t]=i,!0)}function pe(t){return he[t]}function Ei(){return Object.keys(he)}const Pi={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Ii(t,e,i,s){const n=t.resources.length,r=t.random?Math.floor(Math.random()*n):t.index;let o;if(t.random){let m=t.resources.slice(0);for(o=[];m.length>1;){const k=Math.floor(Math.random()*m.length);o.push(m[k]),m=m.slice(0,k).concat(m.slice(k+1))}o=o.concat(m)}else o=t.resources.slice(r).concat(t.resources.slice(0,r));const a=Date.now();let l="pending",c=0,d,u=null,p=[],x=[];typeof s=="function"&&x.push(s);function w(){u&&(clearTimeout(u),u=null)}function E(){l==="pending"&&(l="aborted"),w(),p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function $(m,k){k&&(x=[]),typeof m=="function"&&x.push(m)}function fe(){return{startTime:a,payload:e,status:l,queriesSent:c,queriesPending:p.length,subscribe:$,abort:E}}function P(){l="failed",x.forEach(m=>{m(void 0,d)})}function S(){p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function _(m,k,F){const ee=k!=="success";switch(p=p.filter(I=>I!==m),l){case"pending":break;case"failed":if(ee||!t.dataAfterTimeout)return;break;default:return}if(k==="abort"){d=F,P();return}if(ee){d=F,p.length||(o.length?ge():P());return}if(w(),S(),!t.random){const I=t.resources.indexOf(m.resource);I!==-1&&I!==t.index&&(t.index=I)}l="completed",x.forEach(I=>{I(F)})}function ge(){if(l!=="pending")return;w();const m=o.shift();if(m===void 0){if(p.length){u=setTimeout(()=>{w(),l==="pending"&&(S(),P())},t.timeout);return}P();return}const k={status:"pending",resource:m,callback:(F,ee)=>{_(k,F,ee)}};p.push(k),c++,u=setTimeout(ge,t.rotate),i(m,e,k.callback)}return setTimeout(ge),fe}function Ot(t){const e={...Pi,...t};let i=[];function s(){i=i.filter(o=>o().status==="pending")}function n(o,a,l){const c=Ii(e,o,a,(d,u)=>{s(),l&&l(d,u)});return i.push(c),c}function r(o){return i.find(a=>o(a))||null}return{query:n,find:r,setIndex:o=>{e.index=o},getIndex:()=>e.index,cleanup:s}}function Xe(){}const ve=Object.create(null);function Oi(t){if(!ve[t]){const e=pe(t);if(!e)return;ve[t]={config:e,redundancy:Ot(e)}}return ve[t]}function Dt(t,e,i){let s,n;if(typeof t=="string"){const r=we(t);if(!r)return i(void 0,424),Xe;n=r.send;const o=Oi(t);o&&(s=o.redundancy)}else{const r=De(t);if(r){s=Ot(r);const o=we(t.resources?t.resources[0]:"");o&&(n=o.send)}}return!s||!n?(i(void 0,424),Xe):s.query(e,n,i)().abort}function et(){}function Di(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,ki(t)}))}function ji(t){const e=[],i=[];return t.forEach(s=>{(s.match(St)?e:i).push(s)}),{valid:e,invalid:i}}function H(t,e,i){function s(){const n=t.pendingIcons;e.forEach(r=>{n&&n.delete(r),t.icons[r]||t.missing.add(r)})}if(i&&typeof i=="object")try{if(!Tt(t,i).length){s();return}}catch(n){console.error(n)}s(),Di(t)}function tt(t,e){t instanceof Promise?t.then(i=>{e(i)}).catch(()=>{e(null)}):e(t)}function Ni(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:i,prefix:s}=t,n=t.iconsToLoad;if(delete t.iconsToLoad,!n||!n.length)return;const r=t.loadIcon;if(t.loadIcons&&(n.length>1||!r)){tt(t.loadIcons(n,s,i),c=>{H(t,n,c)});return}if(r){n.forEach(c=>{tt(r(c,s,i),d=>{H(t,[c],d?{prefix:s,icons:{[c]:d}}:null)})});return}const{valid:o,invalid:a}=ji(n);if(a.length&&H(t,a,null),!o.length)return;const l=s.match(St)?we(i):null;if(!l){H(t,o,null);return}l.prepare(i,s,o).forEach(c=>{Dt(i,c,d=>{H(t,c.icons,d)})})}))}const je=(t,e)=>{const i=Ai(Ti(t,!0,Et()));if(!i.pending.length){let a=!0;return e&&setTimeout(()=>{a&&e(i.loaded,i.missing,i.pending,et)}),()=>{a=!1}}const s=Object.create(null),n=[];let r,o;return i.pending.forEach(a=>{const{provider:l,prefix:c}=a;if(c===o&&l===r)return;r=l,o=c,n.push(C(l,c));const d=s[l]||(s[l]=Object.create(null));d[c]||(d[c]=[])}),i.pending.forEach(a=>{const{provider:l,prefix:c,name:d}=a,u=C(l,c),p=u.pendingIcons||(u.pendingIcons=new Set);p.has(d)||(p.add(d),s[l][c].push(d))}),n.forEach(a=>{const l=s[a.provider][a.prefix];l.length&&Ni(a,l)}),e?Ci(e,i,n):et},Mi=t=>new Promise((e,i)=>{const s=typeof t=="string"?X(t,!0):t;if(!s){i(t);return}je([s||t],n=>{if(n.length&&s){const r=G(s);if(r){e({...Z,...r});return}}i(t)})});function it(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Ri(t,e){if(typeof t=="object")return{data:it(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const r=it(t);if(r)return{data:r,value:t}}const i=X(t,!0,!0);if(!i)return{value:t};const s=G(i);if(s!==void 0||!i.prefix)return{value:t,name:i,data:s};const n=je([i],()=>e(t,i,G(i)));return{value:t,name:i,loading:n}}let jt=!1;try{jt=navigator.vendor.indexOf("Apple")===0}catch{}function Li(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(jt||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const Ui=/(-?[0-9.]*[0-9]+[0-9.]*)/g,Fi=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function _e(t,e,i){if(e===1)return t;if(i=i||100,typeof t=="number")return Math.ceil(t*e*i)/i;if(typeof t!="string")return t;const s=t.split(Ui);if(s===null||!s.length)return t;const n=[];let r=s.shift(),o=Fi.test(r);for(;;){if(o){const a=parseFloat(r);isNaN(a)?n.push(r):n.push(Math.ceil(a*e*i)/i)}else n.push(r);if(r=s.shift(),r===void 0)return n.join("");o=!o}}function qi(t,e="defs"){let i="";const s=t.indexOf("<"+e);for(;s>=0;){const n=t.indexOf(">",s),r=t.indexOf("</"+e);if(n===-1||r===-1)break;const o=t.indexOf(">",r);if(o===-1)break;i+=t.slice(n+1,r).trim(),t=t.slice(0,s).trim()+t.slice(o+1)}return{defs:i,content:t}}function zi(t,e){return t?"<defs>"+t+"</defs>"+e:e}function Hi(t,e,i){const s=qi(t);return zi(s.defs,e+s.content+i)}const Ji=t=>t==="unset"||t==="undefined"||t==="none";function Nt(t,e){const i={...Z,...t},s={..._t,...e},n={left:i.left,top:i.top,width:i.width,height:i.height};let r=i.body;[i,s].forEach(E=>{const $=[],fe=E.hFlip,P=E.vFlip;let S=E.rotate;fe?P?S+=2:($.push("translate("+(n.width+n.left).toString()+" "+(0-n.top).toString()+")"),$.push("scale(-1 1)"),n.top=n.left=0):P&&($.push("translate("+(0-n.left).toString()+" "+(n.height+n.top).toString()+")"),$.push("scale(1 -1)"),n.top=n.left=0);let _;switch(S<0&&(S-=Math.floor(S/4)*4),S=S%4,S){case 1:_=n.height/2+n.top,$.unshift("rotate(90 "+_.toString()+" "+_.toString()+")");break;case 2:$.unshift("rotate(180 "+(n.width/2+n.left).toString()+" "+(n.height/2+n.top).toString()+")");break;case 3:_=n.width/2+n.left,$.unshift("rotate(-90 "+_.toString()+" "+_.toString()+")");break}S%2===1&&(n.left!==n.top&&(_=n.left,n.left=n.top,n.top=_),n.width!==n.height&&(_=n.width,n.width=n.height,n.height=_)),$.length&&(r=Hi(r,'<g transform="'+$.join(" ")+'">',"</g>"))});const o=s.width,a=s.height,l=n.width,c=n.height;let d,u;o===null?(u=a===null?"1em":a==="auto"?c:a,d=_e(u,l/c)):(d=o==="auto"?l:o,u=a===null?_e(d,c/l):a==="auto"?c:a);const p={},x=(E,$)=>{Ji($)||(p[E]=$.toString())};x("width",d),x("height",u);const w=[n.left,n.top,l,c];return p.viewBox=w.join(" "),{attributes:p,viewBox:w,body:r}}function Ne(t,e){let i=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const s in e)i+=" "+s+'="'+e[s]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+t+"</svg>"}function Bi(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function Vi(t){return"data:image/svg+xml,"+Bi(t)}function Mt(t){return'url("'+Vi(t)+'")'}const Ki=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let ce=Ki();function Qi(t){ce=t}function Wi(){return ce}function Gi(t,e){const i=pe(t);if(!i)return 0;let s;if(!i.maxURL)s=0;else{let n=0;i.resources.forEach(o=>{n=Math.max(n,o.length)});const r=e+".json?icons=";s=i.maxURL-n-i.path.length-r.length}return s}function Yi(t){return t===404}const Zi=(t,e,i)=>{const s=[],n=Gi(t,e),r="icons";let o={type:r,provider:t,prefix:e,icons:[]},a=0;return i.forEach((l,c)=>{a+=l.length+1,a>=n&&c>0&&(s.push(o),o={type:r,provider:t,prefix:e,icons:[]},a=l.length),o.icons.push(l)}),s.push(o),s};function Xi(t){if(typeof t=="string"){const e=pe(t);if(e)return e.path}return"/"}const es=(t,e,i)=>{if(!ce){i("abort",424);return}let s=Xi(e.provider);switch(e.type){case"icons":{const r=e.prefix,o=e.icons.join(","),a=new URLSearchParams({icons:o});s+=r+".json?"+a.toString();break}case"custom":{const r=e.uri;s+=r.slice(0,1)==="/"?r.slice(1):r;break}default:i("abort",400);return}let n=503;ce(t+s).then(r=>{const o=r.status;if(o!==200){setTimeout(()=>{i(Yi(o)?"abort":"next",o)});return}return n=501,r.json()}).then(r=>{if(typeof r!="object"||r===null){setTimeout(()=>{r===404?i("abort",r):i("next",n)});return}setTimeout(()=>{i("success",r)})}).catch(()=>{i("next",n)})},ts={prepare:Zi,send:es};function is(t,e,i){C(i||"",e).loadIcons=t}function ss(t,e,i){C(i||"",e).loadIcon=t}const ye="data-style";let Rt="";function ns(t){Rt=t}function st(t,e){let i=Array.from(t.childNodes).find(s=>s.hasAttribute&&s.hasAttribute(ye));i||(i=document.createElement("style"),i.setAttribute(ye,ye),t.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Rt}function Lt(){Ye("",ts),Et(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const i=t.IconifyPreload,s="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(n=>{try{(typeof n!="object"||n===null||n instanceof Array||typeof n.icons!="object"||typeof n.prefix!="string"||!Ge(n))&&console.error(s)}catch{console.error(s)}})}if(t.IconifyProviders!==void 0){const i=t.IconifyProviders;if(typeof i=="object"&&i!==null)for(const s in i){const n="IconifyProviders["+s+"] is invalid.";try{const r=i[s];if(typeof r!="object"||!r||r.resources===void 0)continue;Ze(s,r)||console.error(n)}catch{console.error(n)}}}}return{iconLoaded:wi,getIcon:_i,listIcons:$i,addIcon:Pt,addCollection:Ge,calculateSize:_e,buildIcon:Nt,iconToHTML:Ne,svgToURL:Mt,loadIcons:je,loadIcon:Mi,addAPIProvider:Ze,setCustomIconLoader:ss,setCustomIconsLoader:is,appendCustomStyle:ns,_api:{getAPIConfig:pe,setAPIModule:Ye,sendAPIQuery:Dt,setFetch:Qi,getFetch:Wi,listAPIProviders:Ei}}}const ke={"background-color":"currentColor"},Ut={"background-color":"transparent"},nt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},rt={"-webkit-mask":ke,mask:ke,background:Ut};for(const t in rt){const e=rt[t];for(const i in nt)e[t+"-"+i]=nt[i]}function ot(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function rs(t,e,i){const s=document.createElement("span");let n=t.body;n.indexOf("<a")!==-1&&(n+="<!-- "+Date.now()+" -->");const r=t.attributes,o=Ne(n,{...r,width:e.width+"",height:e.height+""}),a=Mt(o),l=s.style,c={"--svg":a,width:ot(r.width),height:ot(r.height),...i?ke:Ut};for(const d in c)l.setProperty(d,c[d]);return s}let B;function os(){try{B=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{B=null}}function as(t){return B===void 0&&os(),B?B.createHTML(t):t}function ls(t){const e=document.createElement("span"),i=t.attributes;let s="";i.width||(s="width: inherit;"),i.height||(s+="height: inherit;"),s&&(i.style=s);const n=Ne(t.body,i);return e.innerHTML=as(n),e.firstChild}function Se(t){return Array.from(t.childNodes).find(e=>{const i=e.tagName&&e.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function at(t,e){const i=e.icon.data,s=e.customisations,n=Nt(i,s);s.preserveAspectRatio&&(n.attributes.preserveAspectRatio=s.preserveAspectRatio);const r=e.renderedMode;let o;r==="svg"?o=ls(n):o=rs(n,{...Z,...i},r==="mask");const a=Se(t);a?o.tagName==="SPAN"&&a.tagName===o.tagName?a.setAttribute("style",o.getAttribute("style")):t.replaceChild(o,a):t.appendChild(o)}function lt(t,e,i){const s=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:s}}function cs(t="iconify-icon"){let e,i;try{e=window.customElements,i=window.HTMLElement}catch{return}if(!e||!i)return;const s=e.get(t);if(s)return s;const n=["icon","mode","inline","noobserver","width","height","rotate","flip"],r=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const a=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");st(a,l),this._state=lt({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return n.slice(0)}attributeChangedCallback(a){switch(a){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,st(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const a=this.getAttribute("icon");if(a&&a.slice(0,1)==="{")try{return JSON.parse(a)}catch{}return a}set icon(a){typeof a=="object"&&(a=JSON.stringify(a)),this.setAttribute("icon",a)}get inline(){return this.hasAttribute("inline")}set inline(a){a?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(a){a?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const a=this._state;if(a.rendered){const l=this._shadowRoot;if(a.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}at(l,a)}}get status(){const a=this._state;return a.rendered?"rendered":a.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const a=this._state,l=this.getAttribute("icon");if(l!==a.icon.value){this._iconChanged(l);return}if(!a.rendered||!this._visible)return;const c=this.getAttribute("mode"),d=Qe(this);(a.attrMode!==c||fi(a.customisations,d)||!Se(this._shadowRoot))&&this._renderIcon(a.icon,d,c)}_iconChanged(a){const l=Ri(a,(c,d,u)=>{const p=this._state;if(p.rendered||this.getAttribute("icon")!==c)return;const x={value:c,name:d,data:u};x.data?this._gotIconData(x):p.icon=x});l.data?this._gotIconData(l):this._state=lt(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const a=Se(this._shadowRoot);a&&this._shadowRoot.removeChild(a);return}this._queueCheck()}_gotIconData(a){this._checkQueued=!1,this._renderIcon(a,Qe(this),this.getAttribute("mode"))}_renderIcon(a,l,c){const d=Li(a.data.body,c),u=this._state.inline;at(this._shadowRoot,this._state={rendered:!0,icon:a,inline:u,customisations:l,attrMode:c,renderedMode:d})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(a=>{const l=a.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};n.forEach(a=>{a in r.prototype||Object.defineProperty(r.prototype,a,{get:function(){return this.getAttribute(a)},set:function(l){l!==null?this.setAttribute(a,l):this.removeAttribute(a)}})});const o=Lt();for(const a in o)r[a]=r.prototype[a]=o[a];return e.define(t,r),r}const ds=cs()||Lt(),{iconLoaded:Ss,getIcon:Cs,listIcons:As,addIcon:Ts,addCollection:Es,calculateSize:Ps,buildIcon:Is,iconToHTML:Os,svgToURL:Ds,loadIcons:js,loadIcon:Ns,setCustomIconLoader:Ms,setCustomIconsLoader:Rs,addAPIProvider:Ls,_api:Us}=ds;async function y(t,e){const i=await fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!i.ok){const s=await i.json().catch(()=>({error:i.statusText}));throw new Error(s.error||i.statusText)}return i.status===204?void 0:i.json()}var us=Object.defineProperty,hs=Object.getOwnPropertyDescriptor,U=(t,e,i,s)=>{for(var n=s>1?void 0:s?hs(e,i):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(n=(s?o(e,i,n):o(n))||n);return s&&n&&us(e,i,n),n};let T=class extends M{constructor(){super(...arguments),this.channelKind="webhook",this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await y("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(t){return String(new FormData(t).get("node_name")??"").trim()}async createCluster(t){if(t.preventDefault(),!window.confirm("Create a new single-Node Cluster?"))return;const e=t.currentTarget;await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName(e)})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,i=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(e),join_link:String(i.get("join_link")??"").trim()})}async choose(t,e){this.saving=!0,this.error="";try{await y(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster()}catch(i){this.fail(i),this.saving=!1}}async waitForCluster(){for(let t=0;t<120;t+=1){await new Promise(e=>window.setTimeout(e,250));try{const e=await y("/api/v1/setup");if(e.cluster_ready){this.changed(e);return}}catch{}}throw new Error("Cluster setup did not finish within 30 seconds")}async createChannel(t){t.preventDefault();const e=new FormData(t.currentTarget),i=this.channelKind==="telegram"?{type:"telegram",name:e.get("name"),bot_token:e.get("bot_token"),chat_id:e.get("chat_id")}:{type:"webhook",name:e.get("name"),url:e.get("url"),headers:{}};await this.createResource("/api/v1/channels",i)}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),i={name:String(e.get("name")),url:String(e.get("url")),method:"GET",accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(e.get("interval")),timeout_seconds:Number(e.get("timeout")),failure_threshold:Number(e.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:e.getAll("channel_id").map(String)};await this.createResource("/api/v1/targets",i)}async createResource(t,e){this.saving=!0;try{await y(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(i){this.fail(i),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await y("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return h`<section class="flow" aria-label="UpGrid setup">
      ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return h`
      <span class="eyebrow">First-run setup</span><h1>Choose your Cluster</h1>
      <p class="lead">Review this Node’s name, then create a new Cluster or use an invitation to join one.</p>
      <div class="panel">
        <form class="choice" @submit=${this.createCluster}>
          <h2>Start a new Cluster</h2><p>This Node becomes the first voting member.</p>
          <label>Node name<input name="node_name" .value=${this.setup.node_name} required /></label>
          <div class="actions"><button type="submit" ?disabled=${this.saving}>${this.saving?"Setting up…":"Create new Cluster"}</button></div>
        </form>
        <form class="choice" @submit=${this.joinCluster}>
          <h2>Join an existing Cluster</h2><p>Paste an <code>up://</code> Join Token from a current member.</p>
          <label>Node name<input name="node_name" .value=${this.setup.node_name} required /></label>
          <label>Join Token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
          <div class="actions"><button class="secondary" type="submit" ?disabled=${this.saving}>Join Cluster</button></div>
        </form>
      </div>`}renderChannel(){return h`
      <span class="eyebrow">Optional · Step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions to Telegram or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createChannel}>
        <label>Type<select name="type" @change=${t=>this.channelKind=t.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
        <label>Name<input name="name" placeholder="On-call" required /></label>
        ${this.channelKind==="webhook"?h`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:h`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and continue</button></div>
      </form></div>`}renderTarget(){return h`
      <span class="eyebrow">Optional · Step 3 of 3</span><h1>Monitor your first Target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createTarget}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label></div>
        <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
        ${this.channels.length?h`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>h`<label><span><input name="channel_id" type="checkbox" value=${t.id} /> ${t.name}</span></label>`)}</fieldset>`:f}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`}};T.styles=ht`
    :host { display: block; }
    .flow { width: min(680px, 100%); margin: 8vh auto 0; }
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .lead { margin: 0 0 24px; color: var(--muted); font-size: 15px; }
    .panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; transition: border-color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .65; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 600px) { .flow { margin-top: 3vh; } .row { grid-template-columns: 1fr; } }
  `;U([vt({attribute:!1})],T.prototype,"setup",2);U([g()],T.prototype,"channelKind",2);U([g()],T.prototype,"channels",2);U([g()],T.prototype,"saving",2);U([g()],T.prototype,"error",2);T=U([bt("upgrid-setup")],T);const ps={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},fs={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},gs={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var ms=Object.defineProperty,v=(t,e,i,s)=>{for(var n=void 0,r=t.length-1,o;r>=0;r--)(o=t[r])&&(n=o(e,i,n)||n);return n&&ms(e,i,n),n};const ne=["system","dark","bright"],ct={system:fs,dark:ps,bright:gs},Me={overview:"/",alerts:"/alerts",cluster:"/cluster"};function dt(){return Object.entries(Me).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function bs(){const t=localStorage.getItem("upgrid-theme");return ne.includes(t)?t:"system"}class b extends M{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.secrets=[],this.joinTokens=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.joinCommand="",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=dt(),this.copied=!1,this.setupMode=!1,this.warningDismissed=!1,this.unlimitedUses=!0,this.theme=bs(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=dt()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await y("/api/v1/setup");if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}catch(e){this.error=e instanceof Error?e.message:String(e)}}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=ne[(ne.indexOf(this.theme)+1)%ne.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}async refresh(){try{[this.targets,this.channels,this.alerts,this.secrets,this.cluster,this.joinTokens]=await Promise.all([y("/api/v1/targets"),y("/api/v1/channels"),y("/api/v1/alerts"),y("/api/v1/secrets"),y("/api/v1/cluster"),y("/api/v1/join-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),s=i?.querySelector("form");s&&(this.detailInitialState=this.detailFormState(s)),i?.showModal()})}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,i){e.preventDefault(),this.activeSection=i,window.history.pushState(null,"",Me[i]),this.updateComplete.then(()=>this.renderRoot.querySelector(`#${i}`)?.scrollIntoView({behavior:"smooth",block:"start"}))}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const i=e.currentTarget,s=i.form?.elements.namedItem("max_redirects");s&&(s.disabled=!i.checked),i.form&&this.compareDetailForm(i.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}}v([g()],b.prototype,"targets");v([g()],b.prototype,"channels");v([g()],b.prototype,"alerts");v([g()],b.prototype,"secrets");v([g()],b.prototype,"cluster");v([g()],b.prototype,"joinTokens");v([g()],b.prototype,"error");v([g()],b.prototype,"live");v([g()],b.prototype,"saving");v([g()],b.prototype,"selected");v([g()],b.prototype,"channelKind");v([g()],b.prototype,"joinCommand");v([g()],b.prototype,"search");v([g()],b.prototype,"statusFilter");v([g()],b.prototype,"sort");v([g()],b.prototype,"selectedIds");v([g()],b.prototype,"activeSection");v([g()],b.prototype,"copied");v([g()],b.prototype,"setupMode");v([g()],b.prototype,"setup");v([g()],b.prototype,"warningDismissed");v([g()],b.prototype,"unlimitedUses");v([g()],b.prototype,"theme");v([g()],b.prototype,"detailDirty");class vs extends b{async createTarget(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i),n={name:String(s.get("name")),url:String(s.get("url")),method:String(s.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(s.get("interval")),timeout_seconds:Number(s.get("timeout")),failure_threshold:Number(s.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:[]};this.saving=!0;try{await y("/api/v1/targets",{method:"POST",body:JSON.stringify(n)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=new FormData(e.currentTarget),s=i.get("follow_redirects")==="on",n={name:String(i.get("name")),url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:String(i.get("statuses")).split(",").map(r=>{const[o,a]=r.trim().split("-").map(Number);return{start:o,end:a||o}}),follow_redirects:s,max_redirects:s?Number(i.get("max_redirects")):0,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([r,o])=>[r,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(i.get("body_contains"))||null,skip_tls_verification:i.get("skip_tls_verification")==="on",notification_channel_ids:this.selected.notification_channel_ids};this.saving=!0;try{await y(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(n)}),this.closeDetailDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await y(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await y(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i);this.saving=!0;try{await y("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:s.get("name"),value:s.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i),n=this.channelKind==="telegram"?{type:"telegram",name:s.get("name"),bot_token:s.get("bot_token"),chat_id:s.get("chat_id")}:{type:"webhook",name:s.get("name"),url:s.get("url"),headers:{}};this.saving=!0;try{await y("/api/v1/channels",{method:"POST",body:JSON.stringify(n)}),i.reset(),this.channelKind="webhook",this.closeDialog("channel-dialog"),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}openTokenDialog(){this.unlimitedUses=!0,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0;try{const s=await y("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(i.get("expiration"))*Number(i.get("unit")),max_uses:this.unlimitedUses?null:Number(i.get("max_uses"))})});this.joinCommand=`upgrid --join '${s.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async setupChanged(e){const i=e.detail;if(this.setup=i,this.setupMode=i.setup,window.history.replaceState(null,"",i.path),i.setup){i.cluster_ready&&(await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await y(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const i=Object.assign(document.createElement("textarea"),{value:this.joinCommand});i.style.cssText="position: fixed; opacity: 0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const s=new Set(this.selectedIds);i?s.add(e):s.delete(e),this.selectedIds=s}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>y(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>y(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,i,s){if(window.confirm(`Delete ${s}?`))try{await y(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}}}const ys={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'};function xs(t,e,i,s){const n=t.accepted_statuses.map(c=>c.start===c.end?c.start:`${c.start}-${c.end}`).join(","),r=t.history.slice(0,30).reverse(),o=Math.max(1,...r.map(c=>c.latency_ms)),a=c=>new Date(c).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),l=c=>c>=1e3?`${(c/1e3).toFixed(c>=1e4?0:1)} s`:`${Math.round(c)} ms`;return h`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${s.backdrop}>
      <div class="dialog-head">
        <h2 id="target-detail-title">Target details</h2>
        <button class="button secondary icon-button dialog-close" type="button" aria-label="Close target details" title="Close" @click=${s.close}><iconify-icon .icon=${$t} aria-hidden="true"></iconify-icon></button>
      </div>
      <form @submit=${s.update} @input=${s.changed}>
        <label>Name<input name="name" .value=${t.name} required /></label>
        <label>URL<input name="url" type="url" .value=${t.url} required /></label>
        <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${n} required /></label></div>
        <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
        <div class="row"><label>Failures before Down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
        <label>Body must contain<input name="body_contains" .value=${t.body_contains??""} /></label>
        <div class="row"><label class="check"><input name="follow_redirects" type="checkbox" .checked=${t.follow_redirects} @change=${s.redirects} />Follow redirects</label><label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${t.skip_tls_verification} />Skip TLS verification</label></div>
        <div class="dialog-actions">
          <div class="danger-actions">
            <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${s.delete}><iconify-icon .icon=${ys} aria-hidden="true"></iconify-icon></button>
            <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>s.pause(!t.paused)}><iconify-icon .icon=${t.paused?xt:yt} aria-hidden="true"></iconify-icon></button>
          </div>
          <button class="button" type="submit" aria-busy=${e?"true":"false"} ?disabled=${e||!i}>Save changes</button>
        </div>
      </form>
      <section class="history">
        <div class="history-head"><h3>Evaluation history</h3>${r.length?h`<span class="meta">Latest ${r.length}</span>`:f}</div>
        ${r.length?h`
          <div class="chart-plot">
            <div class="chart-scale" aria-hidden="true"><span>${l(o)}</span><span>${l(o/2)}</span><span>0 ms</span></div>
            <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${l(o)}`}>
              ${r.map(c=>{const d=c.succeeded?"Passed":"Failed",u=c.status_code===null?"network error":`HTTP ${c.status_code}`,p=`${d} at ${new Date(c.recorded_at_ms).toLocaleString()}: ${c.latency_ms} ms, ${u}`;return h`<span class="history-bar ${c.succeeded?"up":"down"}" role="listitem" aria-label=${p} title=${p} style=${`height: ${Math.max(8,c.latency_ms/o*100)}%`}></span>`})}
            </div>
          </div>
          <div class="chart-axis"><span>${a(r[0].recorded_at_ms)}</span><span>${a(r.at(-1).recorded_at_ms)}</span></div>
          <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
        `:h`<p class="meta">No evaluations recorded yet.</p>`}
      </section>
    </dialog>`}var $s=Object.getOwnPropertyDescriptor,ws=(t,e,i,s)=>{for(var n=s>1?void 0:s?$s(e,i):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(n=o(n)||n);return n};let Ce=class extends vs{render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,i=this.alerts.filter(r=>r.delivery==="pending").length,s=["overview","alerts","cluster"],n=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name));return this.setupMode&&this.setup?h`
        <main class="shell">
          <header>
            <div class="brand">
              <img src="/favicon.svg" alt="" />
              <div><div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"ready":"connecting"}</div></div><span>Distributed service monitoring</span></div>
            </div>
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ct[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>`:h`
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
            ${s.map(r=>h`<a class=${this.activeSection===r?"active":""} href=${Me[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ct[this.theme]} aria-hidden="true"></iconify-icon></button>
          </div>
        </header>
        ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
        ${this.setup?.warning&&!this.warningDismissed?h`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${()=>this.warningDismissed=!0}>Dismiss</button></div>`:f}
        ${this.activeSection==="overview"?this.renderOverview(n,t,e,i):this.activeSection==="alerts"?this.renderAlertsPage():this.renderClusterPage()}
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
      ${this.selected?xs(this.selected,this.saving,this.detailDirty,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),redirects:r=>this.toggleMaxRedirects(r),delete:()=>{this.deleteTarget()},pause:r=>{this.setPaused(r)}}):f}
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
          <label>Type<select name="type" @change=${r=>this.channelKind=r.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind==="webhook"?h`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:h`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("channel-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create Join Token</h2><p>Choose how long the token remains valid and how many Nodes it may admit.</p></div>
        <form @submit=${this.createJoinToken}>
          <div class="row">
            <label>Expiration<input name="expiration" type="number" min="1" step="1" value="1" required /></label>
            <label>Unit<select name="unit"><option value="1">Seconds</option><option value="60">Minutes</option><option value="3600">Hours</option><option value="86400" selected>Days</option></select></label>
          </div>
          <label>Usage<select name="usage" @change=${r=>this.unlimitedUses=r.target.value==="unlimited"}><option value="unlimited">Unlimited</option><option value="limited">Limited</option></select></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join Token Created</h2><p>This command contains Cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderOverview(t,e,i,s){const n=this.targets.filter(a=>this.selectedIds.has(a.id)),r=n.some(a=>!a.paused),o=n.some(a=>a.paused);return h`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="summary" aria-label="Target summary">
        <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
        <div class="metric"><span>Up</span><strong>${e}</strong></div>
        <div class="metric"><span>Down</span><strong>${i}</strong></div>
        <div class="metric"><span>Pending alerts</span><strong>${s}</strong></div>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${a=>this.search=a.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${a=>this.statusFilter=a.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${a=>this.sort=a.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?h`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${$t} aria-hidden="true"></iconify-icon></button>${r?h`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${yt} aria-hidden="true"></iconify-icon></button>`:f}${o?h`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${xt} aria-hidden="true"></iconify-icon></button>`:f}<button class="button danger" @click=${this.bulkDelete}>Delete selected</button></div></div>`:f}
        ${t.length?t.map(a=>this.renderTarget(a)):h`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
      <section class="resources" aria-label="Notification configuration">
        <section class="panel">
          <div class="panel-head"><h2>Notification channels</h2><button class="button secondary" @click=${()=>this.showDialog("channel-dialog")}>Add channel</button></div>
          ${this.channels.length?this.channels.map(a=>h`<div class="resource"><div><strong>${a.name}</strong><code>${a.destination}</code></div><div class="actions"><span class="badge">${a.kind}</span><button class="button danger" aria-label=${`Delete channel ${a.name}`} @click=${()=>this.deleteResource("channels",a.id,a.name)}>Delete</button></div></div>`):h`<div class="empty">No notification channels.</div>`}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length?this.secrets.map(a=>h`<div class="resource"><div><strong>${a.name}</strong><code>${a.id}</code></div><div class="actions"><span class="badge">write-only</span><button class="button danger" aria-label=${`Delete secret ${a.name}`} @click=${()=>this.deleteResource("secrets",a.id,a.name)}>Delete</button></div></div>`):h`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
    `}renderAlertsPage(){return h`
      <section class="heading" id="alerts">
        <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      </section>
      <section class="panel" aria-label="Alert history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${this.alerts.length} events</span></div>
        ${this.alerts.length?this.alerts.map(t=>h`<div class="resource"><div><strong>${t.target_name}</strong><code>${new Date(t.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${t.kind} · ${t.delivery}</span></div>`):h`<div class="empty">No availability transitions.</div>`}
      </section>
    `}renderClusterPage(){return h`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><h2>Nodes</h2><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(t=>h`<div class="resource"><div><strong>${t.name}</strong><code>${t.raft_url}</code></div><div class="actions">${t.local?h`<span class="badge">This node</span>`:f}${t.leader?h`<span class="badge">Leader</span>`:f}</div></div>`)}
        ${this.cluster?.members.length?f:h`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens" style="margin-top: 18px">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(t=>h`
              <div class="resource">
                <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
              </div>
            `):h`<div class="empty">No Join Tokens.</div>`}
      </section>
    `}renderTarget(t){const e=t.latest_evaluation,i=t.history.slice(0,16).reverse(),s=Math.max(1,...i.map(n=>n.latency_ms));return h`
      <div class="target-wrap">
        <input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${n=>this.toggleSelected(t.id,n.target.checked)} />
        <button class="target" aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${t.paused?"paused":t.availability}" aria-label=${t.paused?"paused":t.availability}></i>
          <div>
            <h3>${t.name}</h3>
            <div class="meta">${t.paused?"Paused · ":""}${t.method} · ${t.url} · every ${t.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${i.length?h`<div class="mini-chart" aria-hidden="true">${i.map(n=>h`<i class="mini-bar ${n.succeeded?"up":"down"}" style=${`height: ${Math.max(12,n.latency_ms/s*100)}%`}></i>`)}</div>`:f}
            <div class="latency">
              <strong>${e?`${e.latency_ms} ms`:"—"}</strong>
              <span>${e?e.status_code??"network error":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};Ce.styles=ht`
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
      header { grid-template-columns: minmax(0, 1fr) auto; }
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
  `;Ce=ws([bt("upgrid-app")],Ce);
