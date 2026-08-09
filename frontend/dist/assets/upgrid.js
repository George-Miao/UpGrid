(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const s of document.querySelectorAll('link[rel="modulepreload"]'))n(s);new MutationObserver(s=>{for(const r of s)if(r.type==="childList")for(const o of r.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&n(o)}).observe(document,{childList:!0,subtree:!0});function i(s){const r={};return s.integrity&&(r.integrity=s.integrity),s.referrerPolicy&&(r.referrerPolicy=s.referrerPolicy),s.crossOrigin==="use-credentials"?r.credentials="include":s.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function n(s){if(s.ep)return;s.ep=!0;const r=i(s);fetch(s.href,r)}})();const te=globalThis,Ae=te.ShadowRoot&&(te.ShadyCSS===void 0||te.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Te=Symbol(),Re=new WeakMap;let ut=class{constructor(e,i,n){if(this._$cssResult$=!0,n!==Te)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=i}get styleSheet(){let e=this.o;const i=this.t;if(Ae&&e===void 0){const n=i!==void 0&&i.length===1;n&&(e=Re.get(i)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),n&&Re.set(i,e))}return e}toString(){return this.cssText}};const zt=t=>new ut(typeof t=="string"?t:t+"",void 0,Te),ht=(t,...e)=>{const i=t.length===1?t[0]:e.reduce((n,s,r)=>n+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(s)+t[r+1],t[0]);return new ut(i,t,Te)},Ht=(t,e)=>{if(Ae)t.adoptedStyleSheets=e.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of e){const n=document.createElement("style"),s=te.litNonce;s!==void 0&&n.setAttribute("nonce",s),n.textContent=i.cssText,t.appendChild(n)}},Le=Ae?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let i="";for(const n of e.cssRules)i+=n.cssText;return zt(i)})(t):t;const{is:Jt,defineProperty:Bt,getOwnPropertyDescriptor:Vt,getOwnPropertyNames:Wt,getOwnPropertySymbols:Kt,getPrototypeOf:Qt}=Object,de=globalThis,Ue=de.trustedTypes,Gt=Ue?Ue.emptyScript:"",Yt=de.reactiveElementPolyfillSupport,J=(t,e)=>t,re={toAttribute(t,e){switch(e){case Boolean:t=t?Gt:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let i=t;switch(e){case Boolean:i=t!==null;break;case Number:i=t===null?null:Number(t);break;case Object:case Array:try{i=JSON.parse(t)}catch{i=null}}return i}},Ee=(t,e)=>!Jt(t,e),Fe={attribute:!0,type:String,converter:re,reflect:!1,useDefault:!1,hasChanged:Ee};Symbol.metadata??=Symbol("metadata"),de.litPropertyMetadata??=new WeakMap;let N=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,i=Fe){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(e,i),!i.noAccessor){const n=Symbol(),s=this.getPropertyDescriptor(e,n,i);s!==void 0&&Bt(this.prototype,e,s)}}static getPropertyDescriptor(e,i,n){const{get:s,set:r}=Vt(this.prototype,e)??{get(){return this[i]},set(o){this[i]=o}};return{get:s,set(o){const a=s?.call(this);r?.call(this,o),this.requestUpdate(e,a,n)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??Fe}static _$Ei(){if(this.hasOwnProperty(J("elementProperties")))return;const e=Qt(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(J("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(J("properties"))){const i=this.properties,n=[...Wt(i),...Kt(i)];for(const s of n)this.createProperty(s,i[s])}const e=this[Symbol.metadata];if(e!==null){const i=litPropertyMetadata.get(e);if(i!==void 0)for(const[n,s]of i)this.elementProperties.set(n,s)}this._$Eh=new Map;for(const[i,n]of this.elementProperties){const s=this._$Eu(i,n);s!==void 0&&this._$Eh.set(s,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const i=[];if(Array.isArray(e)){const n=new Set(e.flat(1/0).reverse());for(const s of n)i.unshift(Le(s))}else e!==void 0&&i.push(Le(e));return i}static _$Eu(e,i){const n=i.attribute;return n===!1?void 0:typeof n=="string"?n:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,i=this.constructor.elementProperties;for(const n of i.keys())this.hasOwnProperty(n)&&(e.set(n,this[n]),delete this[n]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Ht(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,i,n){this._$AK(e,n)}_$ET(e,i){const n=this.constructor.elementProperties.get(e),s=this.constructor._$Eu(e,n);if(s!==void 0&&n.reflect===!0){const r=(n.converter?.toAttribute!==void 0?n.converter:re).toAttribute(i,n.type);this._$Em=e,r==null?this.removeAttribute(s):this.setAttribute(s,r),this._$Em=null}}_$AK(e,i){const n=this.constructor,s=n._$Eh.get(e);if(s!==void 0&&this._$Em!==s){const r=n.getPropertyOptions(s),o=typeof r.converter=="function"?{fromAttribute:r.converter}:r.converter?.fromAttribute!==void 0?r.converter:re;this._$Em=s;const a=o.fromAttribute(i,r.type);this[s]=a??this._$Ej?.get(s)??a,this._$Em=null}}requestUpdate(e,i,n,s=!1,r){if(e!==void 0){const o=this.constructor;if(s===!1&&(r=this[e]),n??=o.getPropertyOptions(e),!((n.hasChanged??Ee)(r,i)||n.useDefault&&n.reflect&&r===this._$Ej?.get(e)&&!this.hasAttribute(o._$Eu(e,n))))return;this.C(e,i,n)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,i,{useDefault:n,reflect:s,wrapped:r},o){n&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,o??i??this[e]),r!==!0||o!==void 0)||(this._$AL.has(e)||(this.hasUpdated||n||(i=void 0),this._$AL.set(e,i)),s===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[s,r]of this._$Ep)this[s]=r;this._$Ep=void 0}const n=this.constructor.elementProperties;if(n.size>0)for(const[s,r]of n){const{wrapped:o}=r,a=this[s];o!==!0||this._$AL.has(s)||a===void 0||this.C(s,void 0,r,a)}}let e=!1;const i=this._$AL;try{e=this.shouldUpdate(i),e?(this.willUpdate(i),this._$EO?.forEach(n=>n.hostUpdate?.()),this.update(i)):this._$EM()}catch(n){throw e=!1,this._$EM(),n}e&&this._$AE(i)}willUpdate(e){}_$AE(e){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(e){}firstUpdated(e){}};N.elementStyles=[],N.shadowRootOptions={mode:"open"},N[J("elementProperties")]=new Map,N[J("finalized")]=new Map,Yt?.({ReactiveElement:N}),(de.reactiveElementVersions??=[]).push("2.1.2");const Pe=globalThis,qe=t=>t,oe=Pe.trustedTypes,ze=oe?oe.createPolicy("lit-html",{createHTML:t=>t}):void 0,pt="$lit$",A=`lit$${Math.random().toFixed(9).slice(2)}$`,ft="?"+A,Zt=`<${ft}>`,j=document,V=()=>j.createComment(""),W=t=>t===null||typeof t!="object"&&typeof t!="function",Ie=Array.isArray,Xt=t=>Ie(t)||typeof t?.[Symbol.iterator]=="function",me=`[ 	
\f\r]`,q=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,He=/-->/g,Je=/>/g,O=RegExp(`>|${me}(?:([^\\s"'>=/]+)(${me}*=${me}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),Be=/'/g,Ve=/"/g,gt=/^(?:script|style|textarea|title)$/i,ei=t=>(e,...i)=>({_$litType$:t,strings:e,values:i}),h=ei(1),R=Symbol.for("lit-noChange"),f=Symbol.for("lit-nothing"),We=new WeakMap,D=j.createTreeWalker(j,129);function mt(t,e){if(!Ie(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return ze!==void 0?ze.createHTML(e):e}const ti=(t,e)=>{const i=t.length-1,n=[];let s,r=e===2?"<svg>":e===3?"<math>":"",o=q;for(let a=0;a<i;a++){const l=t[a];let c,d,u=-1,p=0;for(;p<l.length&&(o.lastIndex=p,d=o.exec(l),d!==null);)p=o.lastIndex,o===q?d[1]==="!--"?o=He:d[1]!==void 0?o=Je:d[2]!==void 0?(gt.test(d[2])&&(s=RegExp("</"+d[2],"g")),o=O):d[3]!==void 0&&(o=O):o===O?d[0]===">"?(o=s??q,u=-1):d[1]===void 0?u=-2:(u=o.lastIndex-d[2].length,c=d[1],o=d[3]===void 0?O:d[3]==='"'?Ve:Be):o===Ve||o===Be?o=O:o===He||o===Je?o=q:(o=O,s=void 0);const x=o===O&&t[a+1].startsWith("/>")?" ":"";r+=o===q?l+Zt:u>=0?(n.push(c),l.slice(0,u)+pt+l.slice(u)+A+x):l+A+(u===-2?a:x)}return[mt(t,r+(t[i]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),n]};class K{constructor({strings:e,_$litType$:i},n){let s;this.parts=[];let r=0,o=0;const a=e.length-1,l=this.parts,[c,d]=ti(e,i);if(this.el=K.createElement(c,n),D.currentNode=this.el.content,i===2||i===3){const u=this.el.content.firstChild;u.replaceWith(...u.childNodes)}for(;(s=D.nextNode())!==null&&l.length<a;){if(s.nodeType===1){if(s.hasAttributes())for(const u of s.getAttributeNames())if(u.endsWith(pt)){const p=d[o++],x=s.getAttribute(u).split(A),w=/([.?@])?(.*)/.exec(p);l.push({type:1,index:r,name:w[2],strings:x,ctor:w[1]==="."?si:w[1]==="?"?ni:w[1]==="@"?ri:ue}),s.removeAttribute(u)}else u.startsWith(A)&&(l.push({type:6,index:r}),s.removeAttribute(u));if(gt.test(s.tagName)){const u=s.textContent.split(A),p=u.length-1;if(p>0){s.textContent=oe?oe.emptyScript:"";for(let x=0;x<p;x++)s.append(u[x],V()),D.nextNode(),l.push({type:2,index:++r});s.append(u[p],V())}}}else if(s.nodeType===8)if(s.data===ft)l.push({type:2,index:r});else{let u=-1;for(;(u=s.data.indexOf(A,u+1))!==-1;)l.push({type:7,index:r}),u+=A.length-1}r++}}static createElement(e,i){const n=j.createElement("template");return n.innerHTML=e,n}}function L(t,e,i=t,n){if(e===R)return e;let s=n!==void 0?i._$Co?.[n]:i._$Cl;const r=W(e)?void 0:e._$litDirective$;return s?.constructor!==r&&(s?._$AO?.(!1),r===void 0?s=void 0:(s=new r(t),s._$AT(t,i,n)),n!==void 0?(i._$Co??=[])[n]=s:i._$Cl=s),s!==void 0&&(e=L(t,s._$AS(t,e.values),s,n)),e}class ii{constructor(e,i){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:i},parts:n}=this._$AD,s=(e?.creationScope??j).importNode(i,!0);D.currentNode=s;let r=D.nextNode(),o=0,a=0,l=n[0];for(;l!==void 0;){if(o===l.index){let c;l.type===2?c=new Y(r,r.nextSibling,this,e):l.type===1?c=new l.ctor(r,l.name,l.strings,this,e):l.type===6&&(c=new oi(r,this,e)),this._$AV.push(c),l=n[++a]}o!==l?.index&&(r=D.nextNode(),o++)}return D.currentNode=j,s}p(e){let i=0;for(const n of this._$AV)n!==void 0&&(n.strings!==void 0?(n._$AI(e,n,i),i+=n.strings.length-2):n._$AI(e[i])),i++}}class Y{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,i,n,s){this.type=2,this._$AH=f,this._$AN=void 0,this._$AA=e,this._$AB=i,this._$AM=n,this.options=s,this._$Cv=s?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&e?.nodeType===11&&(e=i.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,i=this){e=L(this,e,i),W(e)?e===f||e==null||e===""?(this._$AH!==f&&this._$AR(),this._$AH=f):e!==this._$AH&&e!==R&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):Xt(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==f&&W(this._$AH)?this._$AA.nextSibling.data=e:this.T(j.createTextNode(e)),this._$AH=e}$(e){const{values:i,_$litType$:n}=e,s=typeof n=="number"?this._$AC(e):(n.el===void 0&&(n.el=K.createElement(mt(n.h,n.h[0]),this.options)),n);if(this._$AH?._$AD===s)this._$AH.p(i);else{const r=new ii(s,this),o=r.u(this.options);r.p(i),this.T(o),this._$AH=r}}_$AC(e){let i=We.get(e.strings);return i===void 0&&We.set(e.strings,i=new K(e)),i}k(e){Ie(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let n,s=0;for(const r of e)s===i.length?i.push(n=new Y(this.O(V()),this.O(V()),this,this.options)):n=i[s],n._$AI(r),s++;s<i.length&&(this._$AR(n&&n._$AB.nextSibling,s),i.length=s)}_$AR(e=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);e!==this._$AB;){const n=qe(e).nextSibling;qe(e).remove(),e=n}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class ue{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,i,n,s,r){this.type=1,this._$AH=f,this._$AN=void 0,this.element=e,this.name=i,this._$AM=s,this.options=r,n.length>2||n[0]!==""||n[1]!==""?(this._$AH=Array(n.length-1).fill(new String),this.strings=n):this._$AH=f}_$AI(e,i=this,n,s){const r=this.strings;let o=!1;if(r===void 0)e=L(this,e,i,0),o=!W(e)||e!==this._$AH&&e!==R,o&&(this._$AH=e);else{const a=e;let l,c;for(e=r[0],l=0;l<r.length-1;l++)c=L(this,a[n+l],i,l),c===R&&(c=this._$AH[l]),o||=!W(c)||c!==this._$AH[l],c===f?e=f:e!==f&&(e+=(c??"")+r[l+1]),this._$AH[l]=c}o&&!s&&this.j(e)}j(e){e===f?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class si extends ue{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===f?void 0:e}}class ni extends ue{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==f)}}class ri extends ue{constructor(e,i,n,s,r){super(e,i,n,s,r),this.type=5}_$AI(e,i=this){if((e=L(this,e,i,0)??f)===R)return;const n=this._$AH,s=e===f&&n!==f||e.capture!==n.capture||e.once!==n.once||e.passive!==n.passive,r=e!==f&&(n===f||s);s&&this.element.removeEventListener(this.name,this,n),r&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class oi{constructor(e,i,n){this.element=e,this.type=6,this._$AN=void 0,this._$AM=i,this.options=n}get _$AU(){return this._$AM._$AU}_$AI(e){L(this,e)}}const ai=Pe.litHtmlPolyfillSupport;ai?.(K,Y),(Pe.litHtmlVersions??=[]).push("3.3.3");const li=(t,e,i)=>{const n=i?.renderBefore??e;let s=n._$litPart$;if(s===void 0){const r=i?.renderBefore??null;n._$litPart$=s=new Y(e.insertBefore(V(),r),r,void 0,i??{})}return s._$AI(t),s};const Oe=globalThis;class M extends N{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=li(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return R}}M._$litElement$=!0,M.finalized=!0,Oe.litElementHydrateSupport?.({LitElement:M});const ci=Oe.litElementPolyfillSupport;ci?.({LitElement:M});(Oe.litElementVersions??=[]).push("4.2.2");const bt=t=>(e,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const di={attribute:!0,type:String,converter:re,reflect:!1,hasChanged:Ee},ui=(t=di,e,i)=>{const{kind:n,metadata:s}=i;let r=globalThis.litPropertyMetadata.get(s);if(r===void 0&&globalThis.litPropertyMetadata.set(s,r=new Map),n==="setter"&&((t=Object.create(t)).wrapped=!0),r.set(i.name,t),n==="accessor"){const{name:o}=i;return{set(a){const l=e.get.call(this);e.set.call(this,a),this.requestUpdate(o,l,t,!0,a)},init(a){return a!==void 0&&this.C(o,void 0,t,a),a}}}if(n==="setter"){const{name:o}=i;return function(a){const l=this[o];e.call(this,a),this.requestUpdate(o,l,t,!0,a)}}throw Error("Unsupported decorator location: "+n)};function vt(t){return(e,i)=>typeof i=="object"?ui(t,e,i):((n,s,r)=>{const o=s.hasOwnProperty(r);return s.constructor.createProperty(r,n),o?Object.getOwnPropertyDescriptor(s,r):void 0})(t,e,i)}function g(t){return vt({...t,state:!0,attribute:!1})}const yt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},xt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},$t={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const wt=Object.freeze({left:0,top:0,width:16,height:16}),ae=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),Z=Object.freeze({...wt,...ae}),xe=Object.freeze({...Z,body:"",hidden:!1}),hi=Object.freeze({width:null,height:null}),_t=Object.freeze({...hi,...ae});function pi(t,e=0){const i=t.replace(/^-?[0-9.]*/,"");function n(s){for(;s<0;)s+=4;return s%4}if(i===""){const s=parseInt(t);return isNaN(s)?0:n(s)}else if(i!==t){let s=0;switch(i){case"%":s=25;break;case"deg":s=90}if(s){let r=parseFloat(t.slice(0,t.length-i.length));return isNaN(r)?0:(r=r/s,r%1===0?n(r):0)}}return e}const fi=/[\s,]+/;function gi(t,e){e.split(fi).forEach(i=>{switch(i.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const kt={..._t,preserveAspectRatio:""};function Ke(t){const e={...kt},i=(n,s)=>t.getAttribute(n)||s;return e.width=i("width",null),e.height=i("height",null),e.rotate=pi(i("rotate","")),gi(e,i("flip","")),e.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),e}function mi(t,e){for(const i in kt)if(t[i]!==e[i])return!0;return!1}const St=/^[a-z0-9]+(-[a-z0-9]+)*$/,X=(t,e,i,n="")=>{const s=t.split(":");if(t.slice(0,1)==="@"){if(s.length<2||s.length>3)return null;n=s.shift().slice(1)}if(s.length>3||!s.length)return null;if(s.length>1){const a=s.pop(),l=s.pop(),c={provider:s.length>0?s[0]:n,prefix:l,name:a};return e&&!ie(c)?null:c}const r=s[0],o=r.split("-");if(o.length>1){const a={provider:n,prefix:o.shift(),name:o.join("-")};return e&&!ie(a)?null:a}if(i&&n===""){const a={provider:n,prefix:"",name:r};return e&&!ie(a,i)?null:a}return null},ie=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function bi(t,e){const i=t.icons,n=t.aliases||Object.create(null),s=Object.create(null);function r(o){if(i[o])return s[o]=[];if(!(o in s)){s[o]=null;const a=n[o]&&n[o].parent,l=a&&r(a);l&&(s[o]=[a].concat(l))}return s[o]}return Object.keys(i).concat(Object.keys(n)).forEach(r),s}function vi(t,e){const i={};!t.hFlip!=!e.hFlip&&(i.hFlip=!0),!t.vFlip!=!e.vFlip&&(i.vFlip=!0);const n=((t.rotate||0)+(e.rotate||0))%4;return n&&(i.rotate=n),i}function Qe(t,e){const i=vi(t,e);for(const n in xe)n in ae?n in t&&!(n in i)&&(i[n]=ae[n]):n in e?i[n]=e[n]:n in t&&(i[n]=t[n]);return i}function yi(t,e,i){const n=t.icons,s=t.aliases||Object.create(null);let r={};function o(a){r=Qe(n[a]||s[a],r)}return o(e),i.forEach(o),Qe(t,r)}function Ct(t,e){const i=[];if(typeof t!="object"||typeof t.icons!="object")return i;t.not_found instanceof Array&&t.not_found.forEach(s=>{e(s,null),i.push(s)});const n=bi(t);for(const s in n){const r=n[s];r&&(e(s,yi(t,s,r)),i.push(s))}return i}const xi={provider:"",aliases:{},not_found:{},...wt};function be(t,e){for(const i in e)if(i in t&&typeof t[i]!=typeof e[i])return!1;return!0}function At(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!be(t,xi))return null;const i=e.icons;for(const s in i){const r=i[s];if(!s||typeof r.body!="string"||!be(r,xe))return null}const n=e.aliases||Object.create(null);for(const s in n){const r=n[s],o=r.parent;if(!s||typeof o!="string"||!i[o]&&!n[o]||!be(r,xe))return null}return e}const le=Object.create(null);function $i(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function C(t,e){const i=le[t]||(le[t]=Object.create(null));return i[e]||(i[e]=$i(t,e))}function Tt(t,e){return At(e)?Ct(e,(i,n)=>{n?t.icons[i]=n:t.missing.add(i)}):[]}function wi(t,e,i){try{if(typeof i.body=="string")return t.icons[e]={...i},!0}catch{}return!1}function _i(t,e){let i=[];return(typeof t=="string"?[t]:Object.keys(le)).forEach(n=>{(typeof n=="string"&&typeof e=="string"?[e]:Object.keys(le[n]||{})).forEach(s=>{const r=C(n,s);i=i.concat(Object.keys(r.icons).map(o=>(n!==""?"@"+n+":":"")+s+":"+o))})}),i}let Q=!1;function Et(t){return typeof t=="boolean"&&(Q=t),Q}function G(t){const e=typeof t=="string"?X(t,!0,Q):t;if(e){const i=C(e.provider,e.prefix),n=e.name;return i.icons[n]||(i.missing.has(n)?null:void 0)}}function Pt(t,e){const i=X(t,!0,Q);if(!i)return!1;const n=C(i.provider,i.prefix);return e?wi(n,i.name,e):(n.missing.add(i.name),!0)}function Ge(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),Q&&!e&&!t.prefix){let n=!1;return At(t)&&(t.prefix="",Ct(t,(s,r)=>{Pt(s,r)&&(n=!0)})),n}const i=t.prefix;return ie({prefix:i,name:"a"})?!!Tt(C(e,i),t):!1}function ki(t){return!!G(t)}function Si(t){const e=G(t);return e&&{...Z,...e}}function It(t,e){t.forEach(i=>{const n=i.loaderCallbacks;n&&(i.loaderCallbacks=n.filter(s=>s.id!==e))})}function Ci(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let i=!1;const n=t.provider,s=t.prefix;e.forEach(r=>{const o=r.icons,a=o.pending.length;o.pending=o.pending.filter(l=>{if(l.prefix!==s)return!0;const c=l.name;if(t.icons[c])o.loaded.push({provider:n,prefix:s,name:c});else if(t.missing.has(c))o.missing.push({provider:n,prefix:s,name:c});else return i=!0,!0;return!1}),o.pending.length!==a&&(i||It([t],r.id),r.callback(o.loaded.slice(0),o.missing.slice(0),o.pending.slice(0),r.abort))})}))}let Ai=0;function Ti(t,e,i){const n=Ai++,s=It.bind(null,i,n);if(!e.pending.length)return s;const r={id:n,icons:e,callback:t,abort:s};return i.forEach(o=>{(o.loaderCallbacks||(o.loaderCallbacks=[])).push(r)}),s}function Ei(t){const e={loaded:[],missing:[],pending:[]},i=Object.create(null);t.sort((s,r)=>s.provider!==r.provider?s.provider.localeCompare(r.provider):s.prefix!==r.prefix?s.prefix.localeCompare(r.prefix):s.name.localeCompare(r.name));let n={provider:"",prefix:"",name:""};return t.forEach(s=>{if(n.name===s.name&&n.prefix===s.prefix&&n.provider===s.provider)return;n=s;const r=s.provider,o=s.prefix,a=s.name,l=i[r]||(i[r]=Object.create(null)),c=l[o]||(l[o]=C(r,o));let d;a in c.icons?d=e.loaded:o===""||c.missing.has(a)?d=e.missing:d=e.pending;const u={provider:r,prefix:o,name:a};d.push(u)}),e}const $e=Object.create(null);function Ye(t,e){$e[t]=e}function we(t){return $e[t]||$e[""]}function Pi(t,e=!0,i=!1){const n=[];return t.forEach(s=>{const r=typeof s=="string"?X(s,e,i):s;r&&n.push(r)}),n}function De(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const he=Object.create(null),z=["https://api.simplesvg.com","https://api.unisvg.com"],se=[];for(;z.length>0;)z.length===1||Math.random()>.5?se.push(z.shift()):se.push(z.pop());he[""]=De({resources:["https://api.iconify.design"].concat(se)});function Ze(t,e){const i=De(e);return i===null?!1:(he[t]=i,!0)}function pe(t){return he[t]}function Ii(){return Object.keys(he)}const Oi={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Di(t,e,i,n){const s=t.resources.length,r=t.random?Math.floor(Math.random()*s):t.index;let o;if(t.random){let m=t.resources.slice(0);for(o=[];m.length>1;){const k=Math.floor(Math.random()*m.length);o.push(m[k]),m=m.slice(0,k).concat(m.slice(k+1))}o=o.concat(m)}else o=t.resources.slice(r).concat(t.resources.slice(0,r));const a=Date.now();let l="pending",c=0,d,u=null,p=[],x=[];typeof n=="function"&&x.push(n);function w(){u&&(clearTimeout(u),u=null)}function E(){l==="pending"&&(l="aborted"),w(),p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function $(m,k){k&&(x=[]),typeof m=="function"&&x.push(m)}function fe(){return{startTime:a,payload:e,status:l,queriesSent:c,queriesPending:p.length,subscribe:$,abort:E}}function P(){l="failed",x.forEach(m=>{m(void 0,d)})}function S(){p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function _(m,k,F){const ee=k!=="success";switch(p=p.filter(I=>I!==m),l){case"pending":break;case"failed":if(ee||!t.dataAfterTimeout)return;break;default:return}if(k==="abort"){d=F,P();return}if(ee){d=F,p.length||(o.length?ge():P());return}if(w(),S(),!t.random){const I=t.resources.indexOf(m.resource);I!==-1&&I!==t.index&&(t.index=I)}l="completed",x.forEach(I=>{I(F)})}function ge(){if(l!=="pending")return;w();const m=o.shift();if(m===void 0){if(p.length){u=setTimeout(()=>{w(),l==="pending"&&(S(),P())},t.timeout);return}P();return}const k={status:"pending",resource:m,callback:(F,ee)=>{_(k,F,ee)}};p.push(k),c++,u=setTimeout(ge,t.rotate),i(m,e,k.callback)}return setTimeout(ge),fe}function Ot(t){const e={...Oi,...t};let i=[];function n(){i=i.filter(o=>o().status==="pending")}function s(o,a,l){const c=Di(e,o,a,(d,u)=>{n(),l&&l(d,u)});return i.push(c),c}function r(o){return i.find(a=>o(a))||null}return{query:s,find:r,setIndex:o=>{e.index=o},getIndex:()=>e.index,cleanup:n}}function Xe(){}const ve=Object.create(null);function ji(t){if(!ve[t]){const e=pe(t);if(!e)return;ve[t]={config:e,redundancy:Ot(e)}}return ve[t]}function Dt(t,e,i){let n,s;if(typeof t=="string"){const r=we(t);if(!r)return i(void 0,424),Xe;s=r.send;const o=ji(t);o&&(n=o.redundancy)}else{const r=De(t);if(r){n=Ot(r);const o=we(t.resources?t.resources[0]:"");o&&(s=o.send)}}return!n||!s?(i(void 0,424),Xe):n.query(e,s,i)().abort}function et(){}function Ni(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,Ci(t)}))}function Mi(t){const e=[],i=[];return t.forEach(n=>{(n.match(St)?e:i).push(n)}),{valid:e,invalid:i}}function H(t,e,i){function n(){const s=t.pendingIcons;e.forEach(r=>{s&&s.delete(r),t.icons[r]||t.missing.add(r)})}if(i&&typeof i=="object")try{if(!Tt(t,i).length){n();return}}catch(s){console.error(s)}n(),Ni(t)}function tt(t,e){t instanceof Promise?t.then(i=>{e(i)}).catch(()=>{e(null)}):e(t)}function Ri(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:i,prefix:n}=t,s=t.iconsToLoad;if(delete t.iconsToLoad,!s||!s.length)return;const r=t.loadIcon;if(t.loadIcons&&(s.length>1||!r)){tt(t.loadIcons(s,n,i),c=>{H(t,s,c)});return}if(r){s.forEach(c=>{tt(r(c,n,i),d=>{H(t,[c],d?{prefix:n,icons:{[c]:d}}:null)})});return}const{valid:o,invalid:a}=Mi(s);if(a.length&&H(t,a,null),!o.length)return;const l=n.match(St)?we(i):null;if(!l){H(t,o,null);return}l.prepare(i,n,o).forEach(c=>{Dt(i,c,d=>{H(t,c.icons,d)})})}))}const je=(t,e)=>{const i=Ei(Pi(t,!0,Et()));if(!i.pending.length){let a=!0;return e&&setTimeout(()=>{a&&e(i.loaded,i.missing,i.pending,et)}),()=>{a=!1}}const n=Object.create(null),s=[];let r,o;return i.pending.forEach(a=>{const{provider:l,prefix:c}=a;if(c===o&&l===r)return;r=l,o=c,s.push(C(l,c));const d=n[l]||(n[l]=Object.create(null));d[c]||(d[c]=[])}),i.pending.forEach(a=>{const{provider:l,prefix:c,name:d}=a,u=C(l,c),p=u.pendingIcons||(u.pendingIcons=new Set);p.has(d)||(p.add(d),n[l][c].push(d))}),s.forEach(a=>{const l=n[a.provider][a.prefix];l.length&&Ri(a,l)}),e?Ti(e,i,s):et},Li=t=>new Promise((e,i)=>{const n=typeof t=="string"?X(t,!0):t;if(!n){i(t);return}je([n||t],s=>{if(s.length&&n){const r=G(n);if(r){e({...Z,...r});return}}i(t)})});function it(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Ui(t,e){if(typeof t=="object")return{data:it(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const r=it(t);if(r)return{data:r,value:t}}const i=X(t,!0,!0);if(!i)return{value:t};const n=G(i);if(n!==void 0||!i.prefix)return{value:t,name:i,data:n};const s=je([i],()=>e(t,i,G(i)));return{value:t,name:i,loading:s}}let jt=!1;try{jt=navigator.vendor.indexOf("Apple")===0}catch{}function Fi(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(jt||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const qi=/(-?[0-9.]*[0-9]+[0-9.]*)/g,zi=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function _e(t,e,i){if(e===1)return t;if(i=i||100,typeof t=="number")return Math.ceil(t*e*i)/i;if(typeof t!="string")return t;const n=t.split(qi);if(n===null||!n.length)return t;const s=[];let r=n.shift(),o=zi.test(r);for(;;){if(o){const a=parseFloat(r);isNaN(a)?s.push(r):s.push(Math.ceil(a*e*i)/i)}else s.push(r);if(r=n.shift(),r===void 0)return s.join("");o=!o}}function Hi(t,e="defs"){let i="";const n=t.indexOf("<"+e);for(;n>=0;){const s=t.indexOf(">",n),r=t.indexOf("</"+e);if(s===-1||r===-1)break;const o=t.indexOf(">",r);if(o===-1)break;i+=t.slice(s+1,r).trim(),t=t.slice(0,n).trim()+t.slice(o+1)}return{defs:i,content:t}}function Ji(t,e){return t?"<defs>"+t+"</defs>"+e:e}function Bi(t,e,i){const n=Hi(t);return Ji(n.defs,e+n.content+i)}const Vi=t=>t==="unset"||t==="undefined"||t==="none";function Nt(t,e){const i={...Z,...t},n={..._t,...e},s={left:i.left,top:i.top,width:i.width,height:i.height};let r=i.body;[i,n].forEach(E=>{const $=[],fe=E.hFlip,P=E.vFlip;let S=E.rotate;fe?P?S+=2:($.push("translate("+(s.width+s.left).toString()+" "+(0-s.top).toString()+")"),$.push("scale(-1 1)"),s.top=s.left=0):P&&($.push("translate("+(0-s.left).toString()+" "+(s.height+s.top).toString()+")"),$.push("scale(1 -1)"),s.top=s.left=0);let _;switch(S<0&&(S-=Math.floor(S/4)*4),S=S%4,S){case 1:_=s.height/2+s.top,$.unshift("rotate(90 "+_.toString()+" "+_.toString()+")");break;case 2:$.unshift("rotate(180 "+(s.width/2+s.left).toString()+" "+(s.height/2+s.top).toString()+")");break;case 3:_=s.width/2+s.left,$.unshift("rotate(-90 "+_.toString()+" "+_.toString()+")");break}S%2===1&&(s.left!==s.top&&(_=s.left,s.left=s.top,s.top=_),s.width!==s.height&&(_=s.width,s.width=s.height,s.height=_)),$.length&&(r=Bi(r,'<g transform="'+$.join(" ")+'">',"</g>"))});const o=n.width,a=n.height,l=s.width,c=s.height;let d,u;o===null?(u=a===null?"1em":a==="auto"?c:a,d=_e(u,l/c)):(d=o==="auto"?l:o,u=a===null?_e(d,c/l):a==="auto"?c:a);const p={},x=(E,$)=>{Vi($)||(p[E]=$.toString())};x("width",d),x("height",u);const w=[s.left,s.top,l,c];return p.viewBox=w.join(" "),{attributes:p,viewBox:w,body:r}}function Ne(t,e){let i=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const n in e)i+=" "+n+'="'+e[n]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+t+"</svg>"}function Wi(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function Ki(t){return"data:image/svg+xml,"+Wi(t)}function Mt(t){return'url("'+Ki(t)+'")'}const Qi=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let ce=Qi();function Gi(t){ce=t}function Yi(){return ce}function Zi(t,e){const i=pe(t);if(!i)return 0;let n;if(!i.maxURL)n=0;else{let s=0;i.resources.forEach(o=>{s=Math.max(s,o.length)});const r=e+".json?icons=";n=i.maxURL-s-i.path.length-r.length}return n}function Xi(t){return t===404}const es=(t,e,i)=>{const n=[],s=Zi(t,e),r="icons";let o={type:r,provider:t,prefix:e,icons:[]},a=0;return i.forEach((l,c)=>{a+=l.length+1,a>=s&&c>0&&(n.push(o),o={type:r,provider:t,prefix:e,icons:[]},a=l.length),o.icons.push(l)}),n.push(o),n};function ts(t){if(typeof t=="string"){const e=pe(t);if(e)return e.path}return"/"}const is=(t,e,i)=>{if(!ce){i("abort",424);return}let n=ts(e.provider);switch(e.type){case"icons":{const r=e.prefix,o=e.icons.join(","),a=new URLSearchParams({icons:o});n+=r+".json?"+a.toString();break}case"custom":{const r=e.uri;n+=r.slice(0,1)==="/"?r.slice(1):r;break}default:i("abort",400);return}let s=503;ce(t+n).then(r=>{const o=r.status;if(o!==200){setTimeout(()=>{i(Xi(o)?"abort":"next",o)});return}return s=501,r.json()}).then(r=>{if(typeof r!="object"||r===null){setTimeout(()=>{r===404?i("abort",r):i("next",s)});return}setTimeout(()=>{i("success",r)})}).catch(()=>{i("next",s)})},ss={prepare:es,send:is};function ns(t,e,i){C(i||"",e).loadIcons=t}function rs(t,e,i){C(i||"",e).loadIcon=t}const ye="data-style";let Rt="";function os(t){Rt=t}function st(t,e){let i=Array.from(t.childNodes).find(n=>n.hasAttribute&&n.hasAttribute(ye));i||(i=document.createElement("style"),i.setAttribute(ye,ye),t.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Rt}function Lt(){Ye("",ss),Et(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const i=t.IconifyPreload,n="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(s=>{try{(typeof s!="object"||s===null||s instanceof Array||typeof s.icons!="object"||typeof s.prefix!="string"||!Ge(s))&&console.error(n)}catch{console.error(n)}})}if(t.IconifyProviders!==void 0){const i=t.IconifyProviders;if(typeof i=="object"&&i!==null)for(const n in i){const s="IconifyProviders["+n+"] is invalid.";try{const r=i[n];if(typeof r!="object"||!r||r.resources===void 0)continue;Ze(n,r)||console.error(s)}catch{console.error(s)}}}}return{iconLoaded:ki,getIcon:Si,listIcons:_i,addIcon:Pt,addCollection:Ge,calculateSize:_e,buildIcon:Nt,iconToHTML:Ne,svgToURL:Mt,loadIcons:je,loadIcon:Li,addAPIProvider:Ze,setCustomIconLoader:rs,setCustomIconsLoader:ns,appendCustomStyle:os,_api:{getAPIConfig:pe,setAPIModule:Ye,sendAPIQuery:Dt,setFetch:Gi,getFetch:Yi,listAPIProviders:Ii}}}const ke={"background-color":"currentColor"},Ut={"background-color":"transparent"},nt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},rt={"-webkit-mask":ke,mask:ke,background:Ut};for(const t in rt){const e=rt[t];for(const i in nt)e[t+"-"+i]=nt[i]}function ot(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function as(t,e,i){const n=document.createElement("span");let s=t.body;s.indexOf("<a")!==-1&&(s+="<!-- "+Date.now()+" -->");const r=t.attributes,o=Ne(s,{...r,width:e.width+"",height:e.height+""}),a=Mt(o),l=n.style,c={"--svg":a,width:ot(r.width),height:ot(r.height),...i?ke:Ut};for(const d in c)l.setProperty(d,c[d]);return n}let B;function ls(){try{B=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{B=null}}function cs(t){return B===void 0&&ls(),B?B.createHTML(t):t}function ds(t){const e=document.createElement("span"),i=t.attributes;let n="";i.width||(n="width: inherit;"),i.height||(n+="height: inherit;"),n&&(i.style=n);const s=Ne(t.body,i);return e.innerHTML=cs(s),e.firstChild}function Se(t){return Array.from(t.childNodes).find(e=>{const i=e.tagName&&e.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function at(t,e){const i=e.icon.data,n=e.customisations,s=Nt(i,n);n.preserveAspectRatio&&(s.attributes.preserveAspectRatio=n.preserveAspectRatio);const r=e.renderedMode;let o;r==="svg"?o=ds(s):o=as(s,{...Z,...i},r==="mask");const a=Se(t);a?o.tagName==="SPAN"&&a.tagName===o.tagName?a.setAttribute("style",o.getAttribute("style")):t.replaceChild(o,a):t.appendChild(o)}function lt(t,e,i){const n=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:n}}function us(t="iconify-icon"){let e,i;try{e=window.customElements,i=window.HTMLElement}catch{return}if(!e||!i)return;const n=e.get(t);if(n)return n;const s=["icon","mode","inline","noobserver","width","height","rotate","flip"],r=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const a=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");st(a,l),this._state=lt({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return s.slice(0)}attributeChangedCallback(a){switch(a){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,st(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const a=this.getAttribute("icon");if(a&&a.slice(0,1)==="{")try{return JSON.parse(a)}catch{}return a}set icon(a){typeof a=="object"&&(a=JSON.stringify(a)),this.setAttribute("icon",a)}get inline(){return this.hasAttribute("inline")}set inline(a){a?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(a){a?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const a=this._state;if(a.rendered){const l=this._shadowRoot;if(a.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}at(l,a)}}get status(){const a=this._state;return a.rendered?"rendered":a.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const a=this._state,l=this.getAttribute("icon");if(l!==a.icon.value){this._iconChanged(l);return}if(!a.rendered||!this._visible)return;const c=this.getAttribute("mode"),d=Ke(this);(a.attrMode!==c||mi(a.customisations,d)||!Se(this._shadowRoot))&&this._renderIcon(a.icon,d,c)}_iconChanged(a){const l=Ui(a,(c,d,u)=>{const p=this._state;if(p.rendered||this.getAttribute("icon")!==c)return;const x={value:c,name:d,data:u};x.data?this._gotIconData(x):p.icon=x});l.data?this._gotIconData(l):this._state=lt(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const a=Se(this._shadowRoot);a&&this._shadowRoot.removeChild(a);return}this._queueCheck()}_gotIconData(a){this._checkQueued=!1,this._renderIcon(a,Ke(this),this.getAttribute("mode"))}_renderIcon(a,l,c){const d=Fi(a.data.body,c),u=this._state.inline;at(this._shadowRoot,this._state={rendered:!0,icon:a,inline:u,customisations:l,attrMode:c,renderedMode:d})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(a=>{const l=a.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};s.forEach(a=>{a in r.prototype||Object.defineProperty(r.prototype,a,{get:function(){return this.getAttribute(a)},set:function(l){l!==null?this.setAttribute(a,l):this.removeAttribute(a)}})});const o=Lt();for(const a in o)r[a]=r.prototype[a]=o[a];return e.define(t,r),r}const hs=us()||Lt(),{iconLoaded:As,getIcon:Ts,listIcons:Es,addIcon:Ps,addCollection:Is,calculateSize:Os,buildIcon:Ds,iconToHTML:js,svgToURL:Ns,loadIcons:Ms,loadIcon:Rs,setCustomIconLoader:Ls,setCustomIconsLoader:Us,addAPIProvider:Fs,_api:qs}=hs;async function y(t,e){const i=await fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!i.ok){const n=await i.json().catch(()=>({error:i.statusText}));throw new Error(n.error||i.statusText)}return i.status===204?void 0:i.json()}function Ft(t,e){return e==="telegram"?{type:"telegram",name:t.get("name"),bot_token:t.get("bot_token"),chat_id:t.get("chat_id")}:{type:"webhook",name:t.get("name"),url:t.get("url"),headers:{}}}function qt(t,e=[]){return{name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:e}}var ps=Object.defineProperty,fs=Object.getOwnPropertyDescriptor,U=(t,e,i,n)=>{for(var s=n>1?void 0:n?fs(e,i):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(s=(n?o(e,i,s):o(s))||s);return n&&s&&ps(e,i,s),s};let T=class extends M{constructor(){super(...arguments),this.channelKind="webhook",this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await y("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){t.preventDefault(),window.confirm("Create a new single-Node Cluster?")&&await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName()})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,i=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(i.get("join_link")??"").trim()})}async choose(t,e){this.saving=!0,this.error="";try{await y(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster()}catch(i){this.fail(i),this.saving=!1}}async waitForCluster(){for(let t=0;t<120;t+=1){await new Promise(e=>window.setTimeout(e,250));try{const e=await y("/api/v1/setup");if(e.cluster_ready){this.changed(e);return}}catch{}}throw new Error("Cluster setup did not finish within 30 seconds")}async createChannel(t){t.preventDefault();const e=new FormData(t.currentTarget),i=Ft(e,this.channelKind);await this.createResource("/api/v1/channels",i)}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),i=qt(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",i)}async createResource(t,e){this.saving=!0;try{await y(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(i){this.fail(i),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await y("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return h`<section class="flow" aria-label="UpGrid setup">
      ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return h`
      <span class="eyebrow">First-run setup</span><h1>Choose your Cluster</h1>
      <p class="lead">Review this Node’s name, then create a new Cluster or use an invitation to join one.</p>
      <div class="cluster-panel">
        <div class="cluster-identity">
          <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} required /></label>
        </div>
        <form class="cluster-create" @submit=${this.createCluster}>
          <div class="cluster-copy"><h2>Start a new Cluster</h2><p>This Node becomes the first voting member.</p></div>
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
        <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
        <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label></div>
        ${this.channels.length?h`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>h`<label><span><input name="channel_id" type="checkbox" value=${t.id} /> ${t.name}</span></label>`)}</fieldset>`:f}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`}};T.styles=ht`
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
    .cluster-create { display: flex; align-items: center; justify-content: space-between; gap: 18px; }
    .cluster-copy h2 { margin: 0; font-size: 17px; }
    .cluster-copy p { margin: 2px 0 0; color: var(--muted); }
    .cluster-divider { display: flex; align-items: center; gap: 12px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .12em; }
    .cluster-divider::before, .cluster-divider::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .cluster-join { display: grid; gap: 10px; }
    .cluster-join-fields { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; }
    .cluster-join-fields label { min-width: 0; }
    .cluster-join-fields button { height: 41px; white-space: nowrap; }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    input, select { width: 100%; min-height: 41px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; transition: border-color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { display: inline-flex; min-height: 41px; align-items: center; justify-content: center; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .65; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 620px) { .row, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-create { align-items: stretch; flex-direction: column; } .cluster-create button, .cluster-join button { justify-self: end; } }
  `;U([vt({attribute:!1})],T.prototype,"setup",2);U([g()],T.prototype,"channelKind",2);U([g()],T.prototype,"channels",2);U([g()],T.prototype,"saving",2);U([g()],T.prototype,"error",2);T=U([bt("upgrid-setup")],T);const gs={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},ms={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},bs={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var vs=Object.defineProperty,v=(t,e,i,n)=>{for(var s=void 0,r=t.length-1,o;r>=0;r--)(o=t[r])&&(s=o(e,i,s)||s);return s&&vs(e,i,s),s};const ne=["system","dark","bright"],ct={system:ms,dark:gs,bright:bs},Me={overview:"/",alerts:"/alerts",cluster:"/cluster"};function dt(){return Object.entries(Me).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function ys(){const t=localStorage.getItem("upgrid-theme");return ne.includes(t)?t:"system"}class b extends M{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.secrets=[],this.joinTokens=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.joinCommand="",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=dt(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!0,this.theme=ys(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=dt()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await y("/api/v1/setup");if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}catch(e){this.error=e instanceof Error?e.message:String(e)}}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=ne[(ne.indexOf(this.theme)+1)%ne.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.channels,this.alerts,this.secrets,this.cluster,this.joinTokens]=await Promise.all([y("/api/v1/targets"),y("/api/v1/channels"),y("/api/v1/alerts"),y("/api/v1/secrets"),y("/api/v1/cluster"),y("/api/v1/join-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),n=i?.querySelector("form");n&&(this.detailInitialState=this.detailFormState(n)),i?.showModal()})}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,i){e.preventDefault(),this.activeSection=i,window.history.pushState(null,"",Me[i]),this.updateComplete.then(()=>this.renderRoot.querySelector(`#${i}`)?.scrollIntoView({behavior:"smooth",block:"start"}))}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const i=e.currentTarget,n=i.form?.elements.namedItem("max_redirects");n&&(n.disabled=!i.checked),i.form&&this.compareDetailForm(i.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}}v([g()],b.prototype,"targets");v([g()],b.prototype,"channels");v([g()],b.prototype,"alerts");v([g()],b.prototype,"secrets");v([g()],b.prototype,"cluster");v([g()],b.prototype,"joinTokens");v([g()],b.prototype,"error");v([g()],b.prototype,"live");v([g()],b.prototype,"saving");v([g()],b.prototype,"selected");v([g()],b.prototype,"channelKind");v([g()],b.prototype,"joinCommand");v([g()],b.prototype,"search");v([g()],b.prototype,"statusFilter");v([g()],b.prototype,"sort");v([g()],b.prototype,"selectedIds");v([g()],b.prototype,"activeSection");v([g()],b.prototype,"copied");v([g()],b.prototype,"setupMode");v([g()],b.prototype,"setup");v([g()],b.prototype,"warningDismissed");v([g()],b.prototype,"unlimitedUses");v([g()],b.prototype,"theme");v([g()],b.prototype,"detailDirty");class xs extends b{async createTarget(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i),s=qt(n);this.saving=!0;try{await y("/api/v1/targets",{method:"POST",body:JSON.stringify(s)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=new FormData(e.currentTarget),n=i.get("follow_redirects")==="on",s={name:String(i.get("name")),url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:String(i.get("statuses")).split(",").map(r=>{const[o,a]=r.trim().split("-").map(Number);return{start:o,end:a||o}}),follow_redirects:n,max_redirects:n?Number(i.get("max_redirects")):0,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([r,o])=>[r,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(i.get("body_contains"))||null,skip_tls_verification:i.get("skip_tls_verification")==="on",notification_channel_ids:this.selected.notification_channel_ids};this.saving=!0;try{await y(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(s)}),this.closeDetailDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await y(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await y(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i);this.saving=!0;try{await y("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:n.get("name"),value:n.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i),s=Ft(n,this.channelKind);this.saving=!0;try{await y("/api/v1/channels",{method:"POST",body:JSON.stringify(s)}),i.reset(),this.channelKind="webhook",this.closeDialog("channel-dialog"),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}openTokenDialog(){this.unlimitedUses=!0,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0;try{const n=await y("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(i.get("expiration"))*Number(i.get("unit")),max_uses:this.unlimitedUses?null:Number(i.get("max_uses"))})});this.joinCommand=`upgrid --join '${n.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async setupChanged(e){const i=e.detail;if(this.setup=i,this.setupMode=i.setup,window.history.replaceState(null,"",i.path),i.setup){i.cluster_ready&&(await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await y(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const i=Object.assign(document.createElement("textarea"),{value:this.joinCommand});i.style.cssText="position: fixed; opacity: 0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const n=new Set(this.selectedIds);i?n.add(e):n.delete(e),this.selectedIds=n}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>y(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>y(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,i,n){if(window.confirm(`Delete ${n}?`))try{await y(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}}}const $s={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'};function ws(t,e,i,n){const s=t.accepted_statuses.map(c=>c.start===c.end?c.start:`${c.start}-${c.end}`).join(","),r=t.history.slice(0,30).reverse(),o=Math.max(1,...r.map(c=>c.latency_ms)),a=c=>new Date(c).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),l=c=>c>=1e3?`${(c/1e3).toFixed(c>=1e4?0:1)} s`:`${Math.round(c)} ms`;return h`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${n.backdrop}>
      <div class="dialog-head">
        <h2 id="target-detail-title">Target details</h2>
        <button class="button secondary icon-button dialog-close" type="button" aria-label="Close target details" title="Close" @click=${n.close}><iconify-icon .icon=${$t} aria-hidden="true"></iconify-icon></button>
      </div>
      <form @submit=${n.update} @input=${n.changed}>
        <label>Name<input name="name" .value=${t.name} required /></label>
        <label>URL<input name="url" type="url" .value=${t.url} required /></label>
        <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${s} required /></label></div>
        <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
        <div class="row"><label>Failures before Down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
        <label>Body must contain<input name="body_contains" .value=${t.body_contains??""} /></label>
        <div class="row"><label class="check"><input name="follow_redirects" type="checkbox" .checked=${t.follow_redirects} @change=${n.redirects} />Follow redirects</label><label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${t.skip_tls_verification} />Skip TLS verification</label></div>
        <div class="dialog-actions">
          <div class="danger-actions">
            <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${n.delete}><iconify-icon .icon=${$s} aria-hidden="true"></iconify-icon></button>
            <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>n.pause(!t.paused)}><iconify-icon .icon=${t.paused?xt:yt} aria-hidden="true"></iconify-icon></button>
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
    </dialog>`}var _s=Object.getOwnPropertyDescriptor,ks=(t,e,i,n)=>{for(var s=n>1?void 0:n?_s(e,i):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(s=o(s)||s);return s};let Ce=class extends xs{render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,i=this.alerts.filter(r=>r.delivery==="pending").length,n=["overview","alerts","cluster"],s=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name));return this.setupMode&&this.setup?h`
        <main class="shell setup-shell">
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
            ${n.map(r=>h`<a class=${this.activeSection===r?"active":""} href=${Me[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ct[this.theme]} aria-hidden="true"></iconify-icon></button>
          </div>
        </header>
        ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
        ${this.setup?.warning&&!this.warningDismissed?h`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:f}
        ${this.activeSection==="overview"?this.renderOverview(s,t,e,i):this.activeSection==="alerts"?this.renderAlertsPage():this.renderClusterPage()}
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
      ${this.selected?ws(this.selected,this.saving,this.detailDirty,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),redirects:r=>this.toggleMaxRedirects(r),delete:()=>{this.deleteTarget()},pause:r=>{this.setPaused(r)}}):f}
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
    `}renderOverview(t,e,i,n){const s=this.targets.filter(a=>this.selectedIds.has(a.id)),r=s.some(a=>!a.paused),o=s.some(a=>a.paused);return h`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="summary" aria-label="Target summary">
        <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
        <div class="metric"><span>Up</span><strong>${e}</strong></div>
        <div class="metric"><span>Down</span><strong>${i}</strong></div>
        <div class="metric"><span>Pending alerts</span><strong>${n}</strong></div>
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
    `}renderTarget(t){const e=t.latest_evaluation,i=t.history.slice(0,16).reverse(),n=Math.max(1,...i.map(s=>s.latency_ms));return h`
      <div class="target-wrap">
        <input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${s=>this.toggleSelected(t.id,s.target.checked)} />
        <button class="target" aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${t.paused?"paused":t.availability}" aria-label=${t.paused?"paused":t.availability}></i>
          <div>
            <h3>${t.name}</h3>
            <div class="meta">${t.paused?"Paused · ":""}${t.method} · ${t.url} · every ${t.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${i.length?h`<div class="mini-chart" aria-hidden="true">${i.map(s=>h`<i class="mini-bar ${s.succeeded?"up":"down"}" style=${`height: ${Math.max(12,s.latency_ms/n*100)}%`}></i>`)}</div>`:f}
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
    .setup-shell { display: grid; min-height: 100vh; grid-template-rows: auto minmax(0, 1fr); padding-top: 20px; padding-bottom: 20px; }
    .setup-shell header { margin-bottom: 18px; }
    .setup-shell upgrid-setup { align-self: center; }
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
  `;Ce=ks([bt("upgrid-app")],Ce);
