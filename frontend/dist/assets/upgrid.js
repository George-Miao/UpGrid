(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const n of document.querySelectorAll('link[rel="modulepreload"]'))i(n);new MutationObserver(n=>{for(const r of n)if(r.type==="childList")for(const o of r.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&i(o)}).observe(document,{childList:!0,subtree:!0});function s(n){const r={};return n.integrity&&(r.integrity=n.integrity),n.referrerPolicy&&(r.referrerPolicy=n.referrerPolicy),n.crossOrigin==="use-credentials"?r.credentials="include":n.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function i(n){if(n.ep)return;n.ep=!0;const r=s(n);fetch(n.href,r)}})();const te=globalThis,Ee=te.ShadowRoot&&(te.ShadyCSS===void 0||te.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Pe=Symbol(),Ue=new WeakMap;let pt=class{constructor(e,s,i){if(this._$cssResult$=!0,i!==Pe)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=s}get styleSheet(){let e=this.o;const s=this.t;if(Ee&&e===void 0){const i=s!==void 0&&s.length===1;i&&(e=Ue.get(s)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),i&&Ue.set(s,e))}return e}toString(){return this.cssText}};const Jt=t=>new pt(typeof t=="string"?t:t+"",void 0,Pe),ft=(t,...e)=>{const s=t.length===1?t[0]:e.reduce((i,n,r)=>i+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(n)+t[r+1],t[0]);return new pt(s,t,Pe)},Bt=(t,e)=>{if(Ee)t.adoptedStyleSheets=e.map(s=>s instanceof CSSStyleSheet?s:s.styleSheet);else for(const s of e){const i=document.createElement("style"),n=te.litNonce;n!==void 0&&i.setAttribute("nonce",n),i.textContent=s.cssText,t.appendChild(i)}},qe=Ee?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let s="";for(const i of e.cssRules)s+=i.cssText;return Jt(s)})(t):t;const{is:Vt,defineProperty:Wt,getOwnPropertyDescriptor:Kt,getOwnPropertyNames:Qt,getOwnPropertySymbols:Gt,getPrototypeOf:Yt}=Object,ue=globalThis,Fe=ue.trustedTypes,Zt=Fe?Fe.emptyScript:"",Xt=ue.reactiveElementPolyfillSupport,J=(t,e)=>t,re={toAttribute(t,e){switch(e){case Boolean:t=t?Zt:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let s=t;switch(e){case Boolean:s=t!==null;break;case Number:s=t===null?null:Number(t);break;case Object:case Array:try{s=JSON.parse(t)}catch{s=null}}return s}},Ie=(t,e)=>!Vt(t,e),ze={attribute:!0,type:String,converter:re,reflect:!1,useDefault:!1,hasChanged:Ie};Symbol.metadata??=Symbol("metadata"),ue.litPropertyMetadata??=new WeakMap;let N=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,s=ze){if(s.state&&(s.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((s=Object.create(s)).wrapped=!0),this.elementProperties.set(e,s),!s.noAccessor){const i=Symbol(),n=this.getPropertyDescriptor(e,i,s);n!==void 0&&Wt(this.prototype,e,n)}}static getPropertyDescriptor(e,s,i){const{get:n,set:r}=Kt(this.prototype,e)??{get(){return this[s]},set(o){this[s]=o}};return{get:n,set(o){const a=n?.call(this);r?.call(this,o),this.requestUpdate(e,a,i)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??ze}static _$Ei(){if(this.hasOwnProperty(J("elementProperties")))return;const e=Yt(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(J("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(J("properties"))){const s=this.properties,i=[...Qt(s),...Gt(s)];for(const n of i)this.createProperty(n,s[n])}const e=this[Symbol.metadata];if(e!==null){const s=litPropertyMetadata.get(e);if(s!==void 0)for(const[i,n]of s)this.elementProperties.set(i,n)}this._$Eh=new Map;for(const[s,i]of this.elementProperties){const n=this._$Eu(s,i);n!==void 0&&this._$Eh.set(n,s)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const s=[];if(Array.isArray(e)){const i=new Set(e.flat(1/0).reverse());for(const n of i)s.unshift(qe(n))}else e!==void 0&&s.push(qe(e));return s}static _$Eu(e,s){const i=s.attribute;return i===!1?void 0:typeof i=="string"?i:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,s=this.constructor.elementProperties;for(const i of s.keys())this.hasOwnProperty(i)&&(e.set(i,this[i]),delete this[i]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Bt(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,s,i){this._$AK(e,i)}_$ET(e,s){const i=this.constructor.elementProperties.get(e),n=this.constructor._$Eu(e,i);if(n!==void 0&&i.reflect===!0){const r=(i.converter?.toAttribute!==void 0?i.converter:re).toAttribute(s,i.type);this._$Em=e,r==null?this.removeAttribute(n):this.setAttribute(n,r),this._$Em=null}}_$AK(e,s){const i=this.constructor,n=i._$Eh.get(e);if(n!==void 0&&this._$Em!==n){const r=i.getPropertyOptions(n),o=typeof r.converter=="function"?{fromAttribute:r.converter}:r.converter?.fromAttribute!==void 0?r.converter:re;this._$Em=n;const a=o.fromAttribute(s,r.type);this[n]=a??this._$Ej?.get(n)??a,this._$Em=null}}requestUpdate(e,s,i,n=!1,r){if(e!==void 0){const o=this.constructor;if(n===!1&&(r=this[e]),i??=o.getPropertyOptions(e),!((i.hasChanged??Ie)(r,s)||i.useDefault&&i.reflect&&r===this._$Ej?.get(e)&&!this.hasAttribute(o._$Eu(e,i))))return;this.C(e,s,i)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,s,{useDefault:i,reflect:n,wrapped:r},o){i&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,o??s??this[e]),r!==!0||o!==void 0)||(this._$AL.has(e)||(this.hasUpdated||i||(s=void 0),this._$AL.set(e,s)),n===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(s){Promise.reject(s)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[n,r]of this._$Ep)this[n]=r;this._$Ep=void 0}const i=this.constructor.elementProperties;if(i.size>0)for(const[n,r]of i){const{wrapped:o}=r,a=this[n];o!==!0||this._$AL.has(n)||a===void 0||this.C(n,void 0,r,a)}}let e=!1;const s=this._$AL;try{e=this.shouldUpdate(s),e?(this.willUpdate(s),this._$EO?.forEach(i=>i.hostUpdate?.()),this.update(s)):this._$EM()}catch(i){throw e=!1,this._$EM(),i}e&&this._$AE(s)}willUpdate(e){}_$AE(e){this._$EO?.forEach(s=>s.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(s=>this._$ET(s,this[s])),this._$EM()}updated(e){}firstUpdated(e){}};N.elementStyles=[],N.shadowRootOptions={mode:"open"},N[J("elementProperties")]=new Map,N[J("finalized")]=new Map,Xt?.({ReactiveElement:N}),(ue.reactiveElementVersions??=[]).push("2.1.2");const De=globalThis,He=t=>t,oe=De.trustedTypes,Je=oe?oe.createPolicy("lit-html",{createHTML:t=>t}):void 0,gt="$lit$",T=`lit$${Math.random().toFixed(9).slice(2)}$`,mt="?"+T,es=`<${mt}>`,j=document,V=()=>j.createComment(""),W=t=>t===null||typeof t!="object"&&typeof t!="function",Oe=Array.isArray,ts=t=>Oe(t)||typeof t?.[Symbol.iterator]=="function",be=`[ 	
\f\r]`,F=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,Be=/-->/g,Ve=/>/g,D=RegExp(`>|${be}(?:([^\\s"'>=/]+)(${be}*=${be}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),We=/'/g,Ke=/"/g,bt=/^(?:script|style|textarea|title)$/i,ss=t=>(e,...s)=>({_$litType$:t,strings:e,values:s}),p=ss(1),R=Symbol.for("lit-noChange"),f=Symbol.for("lit-nothing"),Qe=new WeakMap,O=j.createTreeWalker(j,129);function vt(t,e){if(!Oe(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return Je!==void 0?Je.createHTML(e):e}const is=(t,e)=>{const s=t.length-1,i=[];let n,r=e===2?"<svg>":e===3?"<math>":"",o=F;for(let a=0;a<s;a++){const l=t[a];let c,u,h=-1,d=0;for(;d<l.length&&(o.lastIndex=d,u=o.exec(l),u!==null);)d=o.lastIndex,o===F?u[1]==="!--"?o=Be:u[1]!==void 0?o=Ve:u[2]!==void 0?(bt.test(u[2])&&(n=RegExp("</"+u[2],"g")),o=D):u[3]!==void 0&&(o=D):o===D?u[0]===">"?(o=n??F,h=-1):u[1]===void 0?h=-2:(h=o.lastIndex-u[2].length,c=u[1],o=u[3]===void 0?D:u[3]==='"'?Ke:We):o===Ke||o===We?o=D:o===Be||o===Ve?o=F:(o=D,n=void 0);const x=o===D&&t[a+1].startsWith("/>")?" ":"";r+=o===F?l+es:h>=0?(i.push(c),l.slice(0,h)+gt+l.slice(h)+T+x):l+T+(h===-2?a:x)}return[vt(t,r+(t[s]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),i]};class K{constructor({strings:e,_$litType$:s},i){let n;this.parts=[];let r=0,o=0;const a=e.length-1,l=this.parts,[c,u]=is(e,s);if(this.el=K.createElement(c,i),O.currentNode=this.el.content,s===2||s===3){const h=this.el.content.firstChild;h.replaceWith(...h.childNodes)}for(;(n=O.nextNode())!==null&&l.length<a;){if(n.nodeType===1){if(n.hasAttributes())for(const h of n.getAttributeNames())if(h.endsWith(gt)){const d=u[o++],x=n.getAttribute(h).split(T),w=/([.?@])?(.*)/.exec(d);l.push({type:1,index:r,name:w[2],strings:x,ctor:w[1]==="."?rs:w[1]==="?"?os:w[1]==="@"?as:he}),n.removeAttribute(h)}else h.startsWith(T)&&(l.push({type:6,index:r}),n.removeAttribute(h));if(bt.test(n.tagName)){const h=n.textContent.split(T),d=h.length-1;if(d>0){n.textContent=oe?oe.emptyScript:"";for(let x=0;x<d;x++)n.append(h[x],V()),O.nextNode(),l.push({type:2,index:++r});n.append(h[d],V())}}}else if(n.nodeType===8)if(n.data===mt)l.push({type:2,index:r});else{let h=-1;for(;(h=n.data.indexOf(T,h+1))!==-1;)l.push({type:7,index:r}),h+=T.length-1}r++}}static createElement(e,s){const i=j.createElement("template");return i.innerHTML=e,i}}function L(t,e,s=t,i){if(e===R)return e;let n=i!==void 0?s._$Co?.[i]:s._$Cl;const r=W(e)?void 0:e._$litDirective$;return n?.constructor!==r&&(n?._$AO?.(!1),r===void 0?n=void 0:(n=new r(t),n._$AT(t,s,i)),i!==void 0?(s._$Co??=[])[i]=n:s._$Cl=n),n!==void 0&&(e=L(t,n._$AS(t,e.values),n,i)),e}class ns{constructor(e,s){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=s}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:s},parts:i}=this._$AD,n=(e?.creationScope??j).importNode(s,!0);O.currentNode=n;let r=O.nextNode(),o=0,a=0,l=i[0];for(;l!==void 0;){if(o===l.index){let c;l.type===2?c=new Y(r,r.nextSibling,this,e):l.type===1?c=new l.ctor(r,l.name,l.strings,this,e):l.type===6&&(c=new ls(r,this,e)),this._$AV.push(c),l=i[++a]}o!==l?.index&&(r=O.nextNode(),o++)}return O.currentNode=j,n}p(e){let s=0;for(const i of this._$AV)i!==void 0&&(i.strings!==void 0?(i._$AI(e,i,s),s+=i.strings.length-2):i._$AI(e[s])),s++}}class Y{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,s,i,n){this.type=2,this._$AH=f,this._$AN=void 0,this._$AA=e,this._$AB=s,this._$AM=i,this.options=n,this._$Cv=n?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const s=this._$AM;return s!==void 0&&e?.nodeType===11&&(e=s.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,s=this){e=L(this,e,s),W(e)?e===f||e==null||e===""?(this._$AH!==f&&this._$AR(),this._$AH=f):e!==this._$AH&&e!==R&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):ts(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==f&&W(this._$AH)?this._$AA.nextSibling.data=e:this.T(j.createTextNode(e)),this._$AH=e}$(e){const{values:s,_$litType$:i}=e,n=typeof i=="number"?this._$AC(e):(i.el===void 0&&(i.el=K.createElement(vt(i.h,i.h[0]),this.options)),i);if(this._$AH?._$AD===n)this._$AH.p(s);else{const r=new ns(n,this),o=r.u(this.options);r.p(s),this.T(o),this._$AH=r}}_$AC(e){let s=Qe.get(e.strings);return s===void 0&&Qe.set(e.strings,s=new K(e)),s}k(e){Oe(this._$AH)||(this._$AH=[],this._$AR());const s=this._$AH;let i,n=0;for(const r of e)n===s.length?s.push(i=new Y(this.O(V()),this.O(V()),this,this.options)):i=s[n],i._$AI(r),n++;n<s.length&&(this._$AR(i&&i._$AB.nextSibling,n),s.length=n)}_$AR(e=this._$AA.nextSibling,s){for(this._$AP?.(!1,!0,s);e!==this._$AB;){const i=He(e).nextSibling;He(e).remove(),e=i}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class he{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,s,i,n,r){this.type=1,this._$AH=f,this._$AN=void 0,this.element=e,this.name=s,this._$AM=n,this.options=r,i.length>2||i[0]!==""||i[1]!==""?(this._$AH=Array(i.length-1).fill(new String),this.strings=i):this._$AH=f}_$AI(e,s=this,i,n){const r=this.strings;let o=!1;if(r===void 0)e=L(this,e,s,0),o=!W(e)||e!==this._$AH&&e!==R,o&&(this._$AH=e);else{const a=e;let l,c;for(e=r[0],l=0;l<r.length-1;l++)c=L(this,a[i+l],s,l),c===R&&(c=this._$AH[l]),o||=!W(c)||c!==this._$AH[l],c===f?e=f:e!==f&&(e+=(c??"")+r[l+1]),this._$AH[l]=c}o&&!n&&this.j(e)}j(e){e===f?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class rs extends he{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===f?void 0:e}}class os extends he{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==f)}}class as extends he{constructor(e,s,i,n,r){super(e,s,i,n,r),this.type=5}_$AI(e,s=this){if((e=L(this,e,s,0)??f)===R)return;const i=this._$AH,n=e===f&&i!==f||e.capture!==i.capture||e.once!==i.once||e.passive!==i.passive,r=e!==f&&(i===f||n);n&&this.element.removeEventListener(this.name,this,i),r&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class ls{constructor(e,s,i){this.element=e,this.type=6,this._$AN=void 0,this._$AM=s,this.options=i}get _$AU(){return this._$AM._$AU}_$AI(e){L(this,e)}}const cs=De.litHtmlPolyfillSupport;cs?.(K,Y),(De.litHtmlVersions??=[]).push("3.3.3");const ds=(t,e,s)=>{const i=s?.renderBefore??e;let n=i._$litPart$;if(n===void 0){const r=s?.renderBefore??null;i._$litPart$=n=new Y(e.insertBefore(V(),r),r,void 0,s??{})}return n._$AI(t),n};const je=globalThis;class M extends N{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const s=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=ds(s,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return R}}M._$litElement$=!0,M.finalized=!0,je.litElementHydrateSupport?.({LitElement:M});const us=je.litElementPolyfillSupport;us?.({LitElement:M});(je.litElementVersions??=[]).push("4.2.2");const yt=t=>(e,s)=>{s!==void 0?s.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const hs={attribute:!0,type:String,converter:re,reflect:!1,hasChanged:Ie},ps=(t=hs,e,s)=>{const{kind:i,metadata:n}=s;let r=globalThis.litPropertyMetadata.get(n);if(r===void 0&&globalThis.litPropertyMetadata.set(n,r=new Map),i==="setter"&&((t=Object.create(t)).wrapped=!0),r.set(s.name,t),i==="accessor"){const{name:o}=s;return{set(a){const l=e.get.call(this);e.set.call(this,a),this.requestUpdate(o,l,t,!0,a)},init(a){return a!==void 0&&this.C(o,void 0,t,a),a}}}if(i==="setter"){const{name:o}=s;return function(a){const l=this[o];e.call(this,a),this.requestUpdate(o,l,t,!0,a)}}throw Error("Unsupported decorator location: "+i)};function xt(t){return(e,s)=>typeof s=="object"?ps(t,e,s):((i,n,r)=>{const o=n.hasOwnProperty(r);return n.constructor.createProperty(r,i),o?Object.getOwnPropertyDescriptor(n,r):void 0})(t,e,s)}function g(t){return xt({...t,state:!0,attribute:!1})}const $t={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},wt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},ae={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},_t={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const kt=Object.freeze({left:0,top:0,width:16,height:16}),le=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),Z=Object.freeze({...kt,...le}),$e=Object.freeze({...Z,body:"",hidden:!1}),fs=Object.freeze({width:null,height:null}),St=Object.freeze({...fs,...le});function gs(t,e=0){const s=t.replace(/^-?[0-9.]*/,"");function i(n){for(;n<0;)n+=4;return n%4}if(s===""){const n=parseInt(t);return isNaN(n)?0:i(n)}else if(s!==t){let n=0;switch(s){case"%":n=25;break;case"deg":n=90}if(n){let r=parseFloat(t.slice(0,t.length-s.length));return isNaN(r)?0:(r=r/n,r%1===0?i(r):0)}}return e}const ms=/[\s,]+/;function bs(t,e){e.split(ms).forEach(s=>{switch(s.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const Ct={...St,preserveAspectRatio:""};function Ge(t){const e={...Ct},s=(i,n)=>t.getAttribute(i)||n;return e.width=s("width",null),e.height=s("height",null),e.rotate=gs(s("rotate","")),bs(e,s("flip","")),e.preserveAspectRatio=s("preserveAspectRatio",s("preserveaspectratio","")),e}function vs(t,e){for(const s in Ct)if(t[s]!==e[s])return!0;return!1}const At=/^[a-z0-9]+(-[a-z0-9]+)*$/,X=(t,e,s,i="")=>{const n=t.split(":");if(t.slice(0,1)==="@"){if(n.length<2||n.length>3)return null;i=n.shift().slice(1)}if(n.length>3||!n.length)return null;if(n.length>1){const a=n.pop(),l=n.pop(),c={provider:n.length>0?n[0]:i,prefix:l,name:a};return e&&!se(c)?null:c}const r=n[0],o=r.split("-");if(o.length>1){const a={provider:i,prefix:o.shift(),name:o.join("-")};return e&&!se(a)?null:a}if(s&&i===""){const a={provider:i,prefix:"",name:r};return e&&!se(a,s)?null:a}return null},se=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function ys(t,e){const s=t.icons,i=t.aliases||Object.create(null),n=Object.create(null);function r(o){if(s[o])return n[o]=[];if(!(o in n)){n[o]=null;const a=i[o]&&i[o].parent,l=a&&r(a);l&&(n[o]=[a].concat(l))}return n[o]}return Object.keys(s).concat(Object.keys(i)).forEach(r),n}function xs(t,e){const s={};!t.hFlip!=!e.hFlip&&(s.hFlip=!0),!t.vFlip!=!e.vFlip&&(s.vFlip=!0);const i=((t.rotate||0)+(e.rotate||0))%4;return i&&(s.rotate=i),s}function Ye(t,e){const s=xs(t,e);for(const i in $e)i in le?i in t&&!(i in s)&&(s[i]=le[i]):i in e?s[i]=e[i]:i in t&&(s[i]=t[i]);return s}function $s(t,e,s){const i=t.icons,n=t.aliases||Object.create(null);let r={};function o(a){r=Ye(i[a]||n[a],r)}return o(e),s.forEach(o),Ye(t,r)}function Tt(t,e){const s=[];if(typeof t!="object"||typeof t.icons!="object")return s;t.not_found instanceof Array&&t.not_found.forEach(n=>{e(n,null),s.push(n)});const i=ys(t);for(const n in i){const r=i[n];r&&(e(n,$s(t,n,r)),s.push(n))}return s}const ws={provider:"",aliases:{},not_found:{},...kt};function ve(t,e){for(const s in e)if(s in t&&typeof t[s]!=typeof e[s])return!1;return!0}function Et(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!ve(t,ws))return null;const s=e.icons;for(const n in s){const r=s[n];if(!n||typeof r.body!="string"||!ve(r,$e))return null}const i=e.aliases||Object.create(null);for(const n in i){const r=i[n],o=r.parent;if(!n||typeof o!="string"||!s[o]&&!i[o]||!ve(r,$e))return null}return e}const ce=Object.create(null);function _s(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function A(t,e){const s=ce[t]||(ce[t]=Object.create(null));return s[e]||(s[e]=_s(t,e))}function Pt(t,e){return Et(e)?Tt(e,(s,i)=>{i?t.icons[s]=i:t.missing.add(s)}):[]}function ks(t,e,s){try{if(typeof s.body=="string")return t.icons[e]={...s},!0}catch{}return!1}function Ss(t,e){let s=[];return(typeof t=="string"?[t]:Object.keys(ce)).forEach(i=>{(typeof i=="string"&&typeof e=="string"?[e]:Object.keys(ce[i]||{})).forEach(n=>{const r=A(i,n);s=s.concat(Object.keys(r.icons).map(o=>(i!==""?"@"+i+":":"")+n+":"+o))})}),s}let Q=!1;function It(t){return typeof t=="boolean"&&(Q=t),Q}function G(t){const e=typeof t=="string"?X(t,!0,Q):t;if(e){const s=A(e.provider,e.prefix),i=e.name;return s.icons[i]||(s.missing.has(i)?null:void 0)}}function Dt(t,e){const s=X(t,!0,Q);if(!s)return!1;const i=A(s.provider,s.prefix);return e?ks(i,s.name,e):(i.missing.add(s.name),!0)}function Ze(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),Q&&!e&&!t.prefix){let i=!1;return Et(t)&&(t.prefix="",Tt(t,(n,r)=>{Dt(n,r)&&(i=!0)})),i}const s=t.prefix;return se({prefix:s,name:"a"})?!!Pt(A(e,s),t):!1}function Cs(t){return!!G(t)}function As(t){const e=G(t);return e&&{...Z,...e}}function Ot(t,e){t.forEach(s=>{const i=s.loaderCallbacks;i&&(s.loaderCallbacks=i.filter(n=>n.id!==e))})}function Ts(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let s=!1;const i=t.provider,n=t.prefix;e.forEach(r=>{const o=r.icons,a=o.pending.length;o.pending=o.pending.filter(l=>{if(l.prefix!==n)return!0;const c=l.name;if(t.icons[c])o.loaded.push({provider:i,prefix:n,name:c});else if(t.missing.has(c))o.missing.push({provider:i,prefix:n,name:c});else return s=!0,!0;return!1}),o.pending.length!==a&&(s||Ot([t],r.id),r.callback(o.loaded.slice(0),o.missing.slice(0),o.pending.slice(0),r.abort))})}))}let Es=0;function Ps(t,e,s){const i=Es++,n=Ot.bind(null,s,i);if(!e.pending.length)return n;const r={id:i,icons:e,callback:t,abort:n};return s.forEach(o=>{(o.loaderCallbacks||(o.loaderCallbacks=[])).push(r)}),n}function Is(t){const e={loaded:[],missing:[],pending:[]},s=Object.create(null);t.sort((n,r)=>n.provider!==r.provider?n.provider.localeCompare(r.provider):n.prefix!==r.prefix?n.prefix.localeCompare(r.prefix):n.name.localeCompare(r.name));let i={provider:"",prefix:"",name:""};return t.forEach(n=>{if(i.name===n.name&&i.prefix===n.prefix&&i.provider===n.provider)return;i=n;const r=n.provider,o=n.prefix,a=n.name,l=s[r]||(s[r]=Object.create(null)),c=l[o]||(l[o]=A(r,o));let u;a in c.icons?u=e.loaded:o===""||c.missing.has(a)?u=e.missing:u=e.pending;const h={provider:r,prefix:o,name:a};u.push(h)}),e}const we=Object.create(null);function Xe(t,e){we[t]=e}function _e(t){return we[t]||we[""]}function Ds(t,e=!0,s=!1){const i=[];return t.forEach(n=>{const r=typeof n=="string"?X(n,e,s):n;r&&i.push(r)}),i}function Ne(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const pe=Object.create(null),z=["https://api.simplesvg.com","https://api.unisvg.com"],ie=[];for(;z.length>0;)z.length===1||Math.random()>.5?ie.push(z.shift()):ie.push(z.pop());pe[""]=Ne({resources:["https://api.iconify.design"].concat(ie)});function et(t,e){const s=Ne(e);return s===null?!1:(pe[t]=s,!0)}function fe(t){return pe[t]}function Os(){return Object.keys(pe)}const js={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Ns(t,e,s,i){const n=t.resources.length,r=t.random?Math.floor(Math.random()*n):t.index;let o;if(t.random){let y=t.resources.slice(0);for(o=[];y.length>1;){const k=Math.floor(Math.random()*y.length);o.push(y[k]),y=y.slice(0,k).concat(y.slice(k+1))}o=o.concat(y)}else o=t.resources.slice(r).concat(t.resources.slice(0,r));const a=Date.now();let l="pending",c=0,u,h=null,d=[],x=[];typeof i=="function"&&x.push(i);function w(){h&&(clearTimeout(h),h=null)}function C(){l==="pending"&&(l="aborted"),w(),d.forEach(y=>{y.status==="pending"&&(y.status="aborted")}),d=[]}function $(y,k){k&&(x=[]),typeof y=="function"&&x.push(y)}function ge(){return{startTime:a,payload:e,status:l,queriesSent:c,queriesPending:d.length,subscribe:$,abort:C}}function P(){l="failed",x.forEach(y=>{y(void 0,u)})}function S(){d.forEach(y=>{y.status==="pending"&&(y.status="aborted")}),d=[]}function _(y,k,q){const ee=k!=="success";switch(d=d.filter(I=>I!==y),l){case"pending":break;case"failed":if(ee||!t.dataAfterTimeout)return;break;default:return}if(k==="abort"){u=q,P();return}if(ee){u=q,d.length||(o.length?me():P());return}if(w(),S(),!t.random){const I=t.resources.indexOf(y.resource);I!==-1&&I!==t.index&&(t.index=I)}l="completed",x.forEach(I=>{I(q)})}function me(){if(l!=="pending")return;w();const y=o.shift();if(y===void 0){if(d.length){h=setTimeout(()=>{w(),l==="pending"&&(S(),P())},t.timeout);return}P();return}const k={status:"pending",resource:y,callback:(q,ee)=>{_(k,q,ee)}};d.push(k),c++,h=setTimeout(me,t.rotate),s(y,e,k.callback)}return setTimeout(me),ge}function jt(t){const e={...js,...t};let s=[];function i(){s=s.filter(o=>o().status==="pending")}function n(o,a,l){const c=Ns(e,o,a,(u,h)=>{i(),l&&l(u,h)});return s.push(c),c}function r(o){return s.find(a=>o(a))||null}return{query:n,find:r,setIndex:o=>{e.index=o},getIndex:()=>e.index,cleanup:i}}function tt(){}const ye=Object.create(null);function Ms(t){if(!ye[t]){const e=fe(t);if(!e)return;ye[t]={config:e,redundancy:jt(e)}}return ye[t]}function Nt(t,e,s){let i,n;if(typeof t=="string"){const r=_e(t);if(!r)return s(void 0,424),tt;n=r.send;const o=Ms(t);o&&(i=o.redundancy)}else{const r=Ne(t);if(r){i=jt(r);const o=_e(t.resources?t.resources[0]:"");o&&(n=o.send)}}return!i||!n?(s(void 0,424),tt):i.query(e,n,s)().abort}function st(){}function Rs(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,Ts(t)}))}function Ls(t){const e=[],s=[];return t.forEach(i=>{(i.match(At)?e:s).push(i)}),{valid:e,invalid:s}}function H(t,e,s){function i(){const n=t.pendingIcons;e.forEach(r=>{n&&n.delete(r),t.icons[r]||t.missing.add(r)})}if(s&&typeof s=="object")try{if(!Pt(t,s).length){i();return}}catch(n){console.error(n)}i(),Rs(t)}function it(t,e){t instanceof Promise?t.then(s=>{e(s)}).catch(()=>{e(null)}):e(t)}function Us(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:s,prefix:i}=t,n=t.iconsToLoad;if(delete t.iconsToLoad,!n||!n.length)return;const r=t.loadIcon;if(t.loadIcons&&(n.length>1||!r)){it(t.loadIcons(n,i,s),c=>{H(t,n,c)});return}if(r){n.forEach(c=>{it(r(c,i,s),u=>{H(t,[c],u?{prefix:i,icons:{[c]:u}}:null)})});return}const{valid:o,invalid:a}=Ls(n);if(a.length&&H(t,a,null),!o.length)return;const l=i.match(At)?_e(s):null;if(!l){H(t,o,null);return}l.prepare(s,i,o).forEach(c=>{Nt(s,c,u=>{H(t,c.icons,u)})})}))}const Me=(t,e)=>{const s=Is(Ds(t,!0,It()));if(!s.pending.length){let a=!0;return e&&setTimeout(()=>{a&&e(s.loaded,s.missing,s.pending,st)}),()=>{a=!1}}const i=Object.create(null),n=[];let r,o;return s.pending.forEach(a=>{const{provider:l,prefix:c}=a;if(c===o&&l===r)return;r=l,o=c,n.push(A(l,c));const u=i[l]||(i[l]=Object.create(null));u[c]||(u[c]=[])}),s.pending.forEach(a=>{const{provider:l,prefix:c,name:u}=a,h=A(l,c),d=h.pendingIcons||(h.pendingIcons=new Set);d.has(u)||(d.add(u),i[l][c].push(u))}),n.forEach(a=>{const l=i[a.provider][a.prefix];l.length&&Us(a,l)}),e?Ps(e,s,n):st},qs=t=>new Promise((e,s)=>{const i=typeof t=="string"?X(t,!0):t;if(!i){s(t);return}Me([i||t],n=>{if(n.length&&i){const r=G(i);if(r){e({...Z,...r});return}}s(t)})});function nt(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Fs(t,e){if(typeof t=="object")return{data:nt(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const r=nt(t);if(r)return{data:r,value:t}}const s=X(t,!0,!0);if(!s)return{value:t};const i=G(s);if(i!==void 0||!s.prefix)return{value:t,name:s,data:i};const n=Me([s],()=>e(t,s,G(s)));return{value:t,name:s,loading:n}}let Mt=!1;try{Mt=navigator.vendor.indexOf("Apple")===0}catch{}function zs(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(Mt||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const Hs=/(-?[0-9.]*[0-9]+[0-9.]*)/g,Js=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function ke(t,e,s){if(e===1)return t;if(s=s||100,typeof t=="number")return Math.ceil(t*e*s)/s;if(typeof t!="string")return t;const i=t.split(Hs);if(i===null||!i.length)return t;const n=[];let r=i.shift(),o=Js.test(r);for(;;){if(o){const a=parseFloat(r);isNaN(a)?n.push(r):n.push(Math.ceil(a*e*s)/s)}else n.push(r);if(r=i.shift(),r===void 0)return n.join("");o=!o}}function Bs(t,e="defs"){let s="";const i=t.indexOf("<"+e);for(;i>=0;){const n=t.indexOf(">",i),r=t.indexOf("</"+e);if(n===-1||r===-1)break;const o=t.indexOf(">",r);if(o===-1)break;s+=t.slice(n+1,r).trim(),t=t.slice(0,i).trim()+t.slice(o+1)}return{defs:s,content:t}}function Vs(t,e){return t?"<defs>"+t+"</defs>"+e:e}function Ws(t,e,s){const i=Bs(t);return Vs(i.defs,e+i.content+s)}const Ks=t=>t==="unset"||t==="undefined"||t==="none";function Rt(t,e){const s={...Z,...t},i={...St,...e},n={left:s.left,top:s.top,width:s.width,height:s.height};let r=s.body;[s,i].forEach(C=>{const $=[],ge=C.hFlip,P=C.vFlip;let S=C.rotate;ge?P?S+=2:($.push("translate("+(n.width+n.left).toString()+" "+(0-n.top).toString()+")"),$.push("scale(-1 1)"),n.top=n.left=0):P&&($.push("translate("+(0-n.left).toString()+" "+(n.height+n.top).toString()+")"),$.push("scale(1 -1)"),n.top=n.left=0);let _;switch(S<0&&(S-=Math.floor(S/4)*4),S=S%4,S){case 1:_=n.height/2+n.top,$.unshift("rotate(90 "+_.toString()+" "+_.toString()+")");break;case 2:$.unshift("rotate(180 "+(n.width/2+n.left).toString()+" "+(n.height/2+n.top).toString()+")");break;case 3:_=n.width/2+n.left,$.unshift("rotate(-90 "+_.toString()+" "+_.toString()+")");break}S%2===1&&(n.left!==n.top&&(_=n.left,n.left=n.top,n.top=_),n.width!==n.height&&(_=n.width,n.width=n.height,n.height=_)),$.length&&(r=Ws(r,'<g transform="'+$.join(" ")+'">',"</g>"))});const o=i.width,a=i.height,l=n.width,c=n.height;let u,h;o===null?(h=a===null?"1em":a==="auto"?c:a,u=ke(h,l/c)):(u=o==="auto"?l:o,h=a===null?ke(u,c/l):a==="auto"?c:a);const d={},x=(C,$)=>{Ks($)||(d[C]=$.toString())};x("width",u),x("height",h);const w=[n.left,n.top,l,c];return d.viewBox=w.join(" "),{attributes:d,viewBox:w,body:r}}function Re(t,e){let s=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const i in e)s+=" "+i+'="'+e[i]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+s+">"+t+"</svg>"}function Qs(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function Gs(t){return"data:image/svg+xml,"+Qs(t)}function Lt(t){return'url("'+Gs(t)+'")'}const Ys=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let de=Ys();function Zs(t){de=t}function Xs(){return de}function ei(t,e){const s=fe(t);if(!s)return 0;let i;if(!s.maxURL)i=0;else{let n=0;s.resources.forEach(o=>{n=Math.max(n,o.length)});const r=e+".json?icons=";i=s.maxURL-n-s.path.length-r.length}return i}function ti(t){return t===404}const si=(t,e,s)=>{const i=[],n=ei(t,e),r="icons";let o={type:r,provider:t,prefix:e,icons:[]},a=0;return s.forEach((l,c)=>{a+=l.length+1,a>=n&&c>0&&(i.push(o),o={type:r,provider:t,prefix:e,icons:[]},a=l.length),o.icons.push(l)}),i.push(o),i};function ii(t){if(typeof t=="string"){const e=fe(t);if(e)return e.path}return"/"}const ni=(t,e,s)=>{if(!de){s("abort",424);return}let i=ii(e.provider);switch(e.type){case"icons":{const r=e.prefix,o=e.icons.join(","),a=new URLSearchParams({icons:o});i+=r+".json?"+a.toString();break}case"custom":{const r=e.uri;i+=r.slice(0,1)==="/"?r.slice(1):r;break}default:s("abort",400);return}let n=503;de(t+i).then(r=>{const o=r.status;if(o!==200){setTimeout(()=>{s(ti(o)?"abort":"next",o)});return}return n=501,r.json()}).then(r=>{if(typeof r!="object"||r===null){setTimeout(()=>{r===404?s("abort",r):s("next",n)});return}setTimeout(()=>{s("success",r)})}).catch(()=>{s("next",n)})},ri={prepare:si,send:ni};function oi(t,e,s){A(s||"",e).loadIcons=t}function ai(t,e,s){A(s||"",e).loadIcon=t}const xe="data-style";let Ut="";function li(t){Ut=t}function rt(t,e){let s=Array.from(t.childNodes).find(i=>i.hasAttribute&&i.hasAttribute(xe));s||(s=document.createElement("style"),s.setAttribute(xe,xe),t.appendChild(s)),s.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Ut}function qt(){Xe("",ri),It(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const s=t.IconifyPreload,i="Invalid IconifyPreload syntax.";typeof s=="object"&&s!==null&&(s instanceof Array?s:[s]).forEach(n=>{try{(typeof n!="object"||n===null||n instanceof Array||typeof n.icons!="object"||typeof n.prefix!="string"||!Ze(n))&&console.error(i)}catch{console.error(i)}})}if(t.IconifyProviders!==void 0){const s=t.IconifyProviders;if(typeof s=="object"&&s!==null)for(const i in s){const n="IconifyProviders["+i+"] is invalid.";try{const r=s[i];if(typeof r!="object"||!r||r.resources===void 0)continue;et(i,r)||console.error(n)}catch{console.error(n)}}}}return{iconLoaded:Cs,getIcon:As,listIcons:Ss,addIcon:Dt,addCollection:Ze,calculateSize:ke,buildIcon:Rt,iconToHTML:Re,svgToURL:Lt,loadIcons:Me,loadIcon:qs,addAPIProvider:et,setCustomIconLoader:ai,setCustomIconsLoader:oi,appendCustomStyle:li,_api:{getAPIConfig:fe,setAPIModule:Xe,sendAPIQuery:Nt,setFetch:Zs,getFetch:Xs,listAPIProviders:Os}}}const Se={"background-color":"currentColor"},Ft={"background-color":"transparent"},ot={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},at={"-webkit-mask":Se,mask:Se,background:Ft};for(const t in at){const e=at[t];for(const s in ot)e[t+"-"+s]=ot[s]}function lt(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function ci(t,e,s){const i=document.createElement("span");let n=t.body;n.indexOf("<a")!==-1&&(n+="<!-- "+Date.now()+" -->");const r=t.attributes,o=Re(n,{...r,width:e.width+"",height:e.height+""}),a=Lt(o),l=i.style,c={"--svg":a,width:lt(r.width),height:lt(r.height),...s?Se:Ft};for(const u in c)l.setProperty(u,c[u]);return i}let B;function di(){try{B=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{B=null}}function ui(t){return B===void 0&&di(),B?B.createHTML(t):t}function hi(t){const e=document.createElement("span"),s=t.attributes;let i="";s.width||(i="width: inherit;"),s.height||(i+="height: inherit;"),i&&(s.style=i);const n=Re(t.body,s);return e.innerHTML=ui(n),e.firstChild}function Ce(t){return Array.from(t.childNodes).find(e=>{const s=e.tagName&&e.tagName.toUpperCase();return s==="SPAN"||s==="SVG"})}function ct(t,e){const s=e.icon.data,i=e.customisations,n=Rt(s,i);i.preserveAspectRatio&&(n.attributes.preserveAspectRatio=i.preserveAspectRatio);const r=e.renderedMode;let o;r==="svg"?o=hi(n):o=ci(n,{...Z,...s},r==="mask");const a=Ce(t);a?o.tagName==="SPAN"&&a.tagName===o.tagName?a.setAttribute("style",o.getAttribute("style")):t.replaceChild(o,a):t.appendChild(o)}function dt(t,e,s){const i=s&&(s.rendered?s:s.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:i}}function pi(t="iconify-icon"){let e,s;try{e=window.customElements,s=window.HTMLElement}catch{return}if(!e||!s)return;const i=e.get(t);if(i)return i;const n=["icon","mode","inline","noobserver","width","height","rotate","flip"],r=class extends s{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const a=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");rt(a,l),this._state=dt({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return n.slice(0)}attributeChangedCallback(a){switch(a){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,rt(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const a=this.getAttribute("icon");if(a&&a.slice(0,1)==="{")try{return JSON.parse(a)}catch{}return a}set icon(a){typeof a=="object"&&(a=JSON.stringify(a)),this.setAttribute("icon",a)}get inline(){return this.hasAttribute("inline")}set inline(a){a?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(a){a?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const a=this._state;if(a.rendered){const l=this._shadowRoot;if(a.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}ct(l,a)}}get status(){const a=this._state;return a.rendered?"rendered":a.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const a=this._state,l=this.getAttribute("icon");if(l!==a.icon.value){this._iconChanged(l);return}if(!a.rendered||!this._visible)return;const c=this.getAttribute("mode"),u=Ge(this);(a.attrMode!==c||vs(a.customisations,u)||!Ce(this._shadowRoot))&&this._renderIcon(a.icon,u,c)}_iconChanged(a){const l=Fs(a,(c,u,h)=>{const d=this._state;if(d.rendered||this.getAttribute("icon")!==c)return;const x={value:c,name:u,data:h};x.data?this._gotIconData(x):d.icon=x});l.data?this._gotIconData(l):this._state=dt(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const a=Ce(this._shadowRoot);a&&this._shadowRoot.removeChild(a);return}this._queueCheck()}_gotIconData(a){this._checkQueued=!1,this._renderIcon(a,Ge(this),this.getAttribute("mode"))}_renderIcon(a,l,c){const u=zs(a.data.body,c),h=this._state.inline;ct(this._shadowRoot,this._state={rendered:!0,icon:a,inline:h,customisations:l,attrMode:c,renderedMode:u})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(a=>{const l=a.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};n.forEach(a=>{a in r.prototype||Object.defineProperty(r.prototype,a,{get:function(){return this.getAttribute(a)},set:function(l){l!==null?this.setAttribute(a,l):this.removeAttribute(a)}})});const o=qt();for(const a in o)r[a]=r.prototype[a]=o[a];return e.define(t,r),r}const fi=pi()||qt(),{iconLoaded:Pi,getIcon:Ii,listIcons:Di,addIcon:Oi,addCollection:ji,calculateSize:Ni,buildIcon:Mi,iconToHTML:Ri,svgToURL:Li,loadIcons:Ui,loadIcon:qi,setCustomIconLoader:Fi,setCustomIconsLoader:zi,addAPIProvider:Hi,_api:Ji}=fi;async function v(t,e){const s=await fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!s.ok){const i=await s.json().catch(()=>({error:s.statusText}));throw new Error(i.error||s.statusText)}return s.status===204?void 0:s.json()}function Ae(t,e){return e==="telegram"?{type:"telegram",name:t.get("name"),bot_token:t.get("bot_token"),chat_id:t.get("chat_id"),default:t.get("default")==="on"}:{type:"webhook",name:t.get("name"),url:t.get("url"),headers:{},default:t.get("default")==="on"}}function zt(t,e=[],s=!0){return{name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:e,use_default_channels:s}}var gi=Object.defineProperty,mi=Object.getOwnPropertyDescriptor,U=(t,e,s,i)=>{for(var n=i>1?void 0:i?mi(e,s):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(n=(i?o(e,s,n):o(n))||n);return i&&n&&gi(e,s,n),n};let E=class extends M{constructor(){super(...arguments),this.channelKind="webhook",this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await v("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){t.preventDefault(),window.confirm("Create a new single-Node Cluster?")&&await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName()})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,s=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(s.get("join_link")??"").trim()})}async choose(t,e){this.saving=!0,this.error="";try{await v(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster()}catch(s){this.fail(s),this.saving=!1}}async waitForCluster(){for(let t=0;t<120;t+=1){await new Promise(e=>window.setTimeout(e,250));try{const e=await v("/api/v1/setup");if(e.cluster_ready){this.changed(e);return}}catch{}}throw new Error("Cluster setup did not finish within 30 seconds")}async createChannel(t){t.preventDefault();const e=new FormData(t.currentTarget),s=Ae(e,this.channelKind);await this.createResource("/api/v1/channels",s)}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),s=zt(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",s)}async createResource(t,e){this.saving=!0;try{await v(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(s){this.fail(s),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await v("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return p`<section class="flow" aria-label="UpGrid setup">
      ${this.error?p`<div class="notice" role="alert">${this.error}</div>`:f}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return p`
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
      </div>`}renderChannel(){return p`
      <span class="eyebrow">Optional · Step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions to Telegram or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createChannel}>
        <label>Type<select name="type" @change=${t=>this.channelKind=t.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
        <label>Name<input name="name" placeholder="On-call" required /></label>
        ${this.channelKind==="webhook"?p`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:p`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
        <label><span><input name="default" type="checkbox" checked /> Default channel</span></label>
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and continue</button></div>
      </form></div>`}renderTarget(){return p`
      <span class="eyebrow">Optional · Step 3 of 3</span><h1>Monitor your first Target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createTarget}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
        <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label></div>
        ${this.channels.length?p`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>p`<label><span><input name="channel_id" type="checkbox" value=${t.id} /> ${t.name}</span></label>`)}</fieldset>`:f}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`}};E.styles=ft`
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
  `;U([xt({attribute:!1})],E.prototype,"setup",2);U([g()],E.prototype,"channelKind",2);U([g()],E.prototype,"channels",2);U([g()],E.prototype,"saving",2);U([g()],E.prototype,"error",2);E=U([yt("upgrid-setup")],E);const bi={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},vi={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},yi={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var xi=Object.defineProperty,b=(t,e,s,i)=>{for(var n=void 0,r=t.length-1,o;r>=0;r--)(o=t[r])&&(n=o(e,s,n)||n);return n&&xi(e,s,n),n};const ne=["system","dark","bright"],ut={system:vi,dark:bi,bright:yi},Le={overview:"/",alerts:"/alerts",cluster:"/cluster"};function ht(){return Object.entries(Le).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function $i(){const t=localStorage.getItem("upgrid-theme");return ne.includes(t)?t:"system"}class m extends M{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.transitions=[],this.secrets=[],this.joinTokens=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.channelTestMessage="",this.testingChannel=!1,this.joinCommand="",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=ht(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!1,this.theme=$i(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=ht()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await v("/api/v1/setup");if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}catch(e){this.error=e instanceof Error?e.message:String(e)}}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=ne[(ne.indexOf(this.theme)+1)%ne.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.channels,this.alerts,this.transitions,this.secrets,this.cluster,this.joinTokens]=await Promise.all([v("/api/v1/targets"),v("/api/v1/channels"),v("/api/v1/alerts"),v("/api/v1/transitions"),v("/api/v1/secrets"),v("/api/v1/cluster"),v("/api/v1/join-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.updateComplete.then(()=>{const s=this.renderRoot.querySelector("#detail-dialog"),i=s?.querySelector("form");i&&(this.detailInitialState=this.detailFormState(i)),s?.showModal()})}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const s=e.currentTarget;e.target===s&&(s.close(),s.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,s){e.preventDefault(),this.activeSection=s,window.history.pushState(null,"",Le[s]),this.updateComplete.then(()=>this.renderRoot.querySelector(`#${s}`)?.scrollIntoView({behavior:"smooth",block:"start"}))}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const s=e.currentTarget,i=s.form?.elements.namedItem("max_redirects");i&&(i.disabled=!s.checked),s.form&&this.compareDetailForm(s.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}}b([g()],m.prototype,"targets");b([g()],m.prototype,"channels");b([g()],m.prototype,"alerts");b([g()],m.prototype,"transitions");b([g()],m.prototype,"secrets");b([g()],m.prototype,"cluster");b([g()],m.prototype,"joinTokens");b([g()],m.prototype,"error");b([g()],m.prototype,"live");b([g()],m.prototype,"saving");b([g()],m.prototype,"selected");b([g()],m.prototype,"channelKind");b([g()],m.prototype,"channelTestMessage");b([g()],m.prototype,"testingChannel");b([g()],m.prototype,"joinCommand");b([g()],m.prototype,"search");b([g()],m.prototype,"statusFilter");b([g()],m.prototype,"sort");b([g()],m.prototype,"selectedIds");b([g()],m.prototype,"activeSection");b([g()],m.prototype,"copied");b([g()],m.prototype,"setupMode");b([g()],m.prototype,"setup");b([g()],m.prototype,"warningDismissed");b([g()],m.prototype,"unlimitedUses");b([g()],m.prototype,"theme");b([g()],m.prototype,"detailDirty");class wi extends m{async createTarget(e){e.preventDefault();const s=e.currentTarget,i=new FormData(s),n=zt(i,i.getAll("channel_id").map(String),i.get("use_default_channels")==="on");this.saving=!0;try{await v("/api/v1/targets",{method:"POST",body:JSON.stringify(n)}),s.reset(),this.closeTargetDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const s=new FormData(e.currentTarget),i=s.get("follow_redirects")==="on",n={name:String(s.get("name")),url:String(s.get("url")),method:String(s.get("method")),accepted_statuses:String(s.get("statuses")).split(",").map(r=>{const[o,a]=r.trim().split("-").map(Number);return{start:o,end:a||o}}),follow_redirects:i,max_redirects:i?Number(s.get("max_redirects")):0,interval_seconds:Number(s.get("interval")),timeout_seconds:Number(s.get("timeout")),failure_threshold:Number(s.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([r,o])=>[r,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(s.get("body_contains"))||null,skip_tls_verification:s.get("skip_tls_verification")==="on",notification_channel_ids:s.getAll("channel_id").map(String),use_default_channels:s.get("use_default_channels")==="on"};this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(n)}),this.closeDetailDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await v(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const s=e.currentTarget,i=new FormData(s);this.saving=!0;try{await v("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:i.get("name"),value:i.get("value")})}),s.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const s=e.currentTarget,i=new FormData(s),n=Ae(i,this.channelKind);this.saving=!0;try{await v("/api/v1/channels",{method:"POST",body:JSON.stringify(n)}),s.reset(),this.channelKind="webhook",this.channelTestMessage="",this.closeDialog("channel-dialog"),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}openChannelDialog(){this.channelTestMessage="",this.showDialog("channel-dialog")}async setChannelDefault(e,s){try{await v(`/api/v1/channels/${e.id}/default`,{method:"PUT",body:JSON.stringify({default:s})}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}}async testChannel(e){const s=e.currentTarget.form;if(!(!s||![...s.querySelectorAll("[data-test-required]")].every(n=>n.reportValidity()))){this.testingChannel=!0,this.channelTestMessage="";try{const n=Ae(new FormData(s),this.channelKind);await v("/api/v1/channels/test",{method:"POST",body:JSON.stringify(n)}),this.channelTestMessage="Test sent"}catch(n){const r=n instanceof Error?n.message:String(n);this.channelTestMessage=`Test failed: ${r}`}finally{this.testingChannel=!1}}}openTokenDialog(){this.unlimitedUses=!1,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const s=new FormData(e.currentTarget);this.saving=!0;try{const i=await v("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(s.get("expiration_days"))*86400,max_uses:this.unlimitedUses?null:Number(s.get("max_uses"))})});this.joinCommand=`upgrid --join '${i.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async setupChanged(e){const s=e.detail;if(this.setup=s,this.setupMode=s.setup,window.history.replaceState(null,"",s.path),s.setup){s.cluster_ready&&(await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await v(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const s=Object.assign(document.createElement("textarea"),{value:this.joinCommand});s.style.cssText="position: fixed; opacity: 0",document.body.append(s),s.select(),e=document.execCommand("copy"),s.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,s){const i=new Set(this.selectedIds);s?i.add(e):i.delete(e),this.selectedIds=i}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(s=>v(`/api/v1/targets/${s}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>v(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,s,i){if(window.confirm(`Delete ${i}?`))try{await v(`/api/v1/${e}/${s}`,{method:"DELETE"}),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}}}function _i(t,e,s){return p`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${s.create}>Add channel</button>
    </section>
    <div class="page-columns">
      <section class="panel" aria-label="Alert history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${t.length} events</span></div>
        ${t.length?t.map(i=>p`<div class="resource"><div><strong>${i.target_name}</strong><code>${new Date(i.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${i.kind}</span></div>`):p`<div class="empty">No availability transitions.</div>`}
      </section>
      <section class="panel" aria-label="Notification channels">
        <div class="panel-head"><h2>Notification channels</h2><span class="meta">${e.length} configured</span></div>
        ${e.length?e.map(i=>p`
              <div class="resource">
                <div><div class="actions"><strong>${i.name}</strong><span class="badge">${i.kind}</span></div><code>${i.destination}</code></div>
                <div class="actions">
                  <label class="switch"><span>Default</span><input type="checkbox" role="switch" aria-label=${`Default channel ${i.name}`} .checked=${i.default} @change=${n=>s.setDefault(i,n.target.checked)} /></label>
                  <button class="button danger icon-button" aria-label=${`Delete channel ${i.name}`} title=${`Delete ${i.name}`} @click=${()=>s.remove(i)}><iconify-icon .icon=${ae} aria-hidden="true"></iconify-icon></button>
                </div>
              </div>
            `):p`<div class="empty">No notification channels.</div>`}
      </section>
    </div>
  `}function Ht(t,e=[],s=!0){return p`
    <fieldset class="channel-fields">
      <legend>Notification channels</legend>
      <label class="switch">
        <span>Use default channels</span>
        <input
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${s}
        />
      </label>
      <div class="channel-options">
        ${t.map(i=>p`
          <label class="check">
            <input
              name="channel_id"
              type="checkbox"
              value=${i.id}
              .checked=${e.includes(i.id)}
            />
            ${i.name} <span class="badge">${i.kind}</span>
          </label>
        `)}
      </div>
    </fieldset>`}function ki(t,e,s){return p`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${s.backdrop}>
      <div class="dialog-head"><h2 id="add-target-title">Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
      <form @submit=${s.create}>
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
        ${Ht(t)}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${s.close}>Cancel</button>
          <button class="button" type="submit" ?disabled=${e}>${e?"Creating…":"Create target"}</button>
        </div>
      </form>
    </dialog>`}function Si(t,e,s,i,n,r){const o=t.accepted_statuses.map(d=>d.start===d.end?d.start:`${d.start}-${d.end}`).join(","),a=t.history.slice(0,30).reverse(),l=Math.max(1,...a.map(d=>d.latency_ms)),c=new Map(i.map(d=>[d.id,d.name])),u=d=>new Date(d).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),h=d=>d>=1e3?`${(d/1e3).toFixed(d>=1e4?0:1)} s`:`${Math.round(d)} ms`;return p`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${r.backdrop}>
      <div class="dialog-head">
        <h2 id="target-detail-title">Target details</h2>
        <button class="button secondary icon-button dialog-close" type="button" aria-label="Close target details" title="Close" @click=${r.close}><iconify-icon .icon=${_t} aria-hidden="true"></iconify-icon></button>
      </div>
      <form @submit=${r.update} @input=${r.changed}>
        <label>Name<input name="name" .value=${t.name} required /></label>
        <label>URL<input name="url" type="url" .value=${t.url} required /></label>
        <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${o} required /></label></div>
        <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
        <div class="row"><label>Failures before Down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
        <label>Body must contain<input name="body_contains" .value=${t.body_contains??""} /></label>
        <div class="row"><label class="check"><input name="follow_redirects" type="checkbox" .checked=${t.follow_redirects} @change=${r.redirects} />Follow redirects</label><label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${t.skip_tls_verification} />Skip TLS verification</label></div>
        ${Ht(n,t.notification_channel_ids,t.use_default_channels)}
        <div class="dialog-actions">
          <div class="danger-actions">
            <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${r.delete}><iconify-icon .icon=${ae} aria-hidden="true"></iconify-icon></button>
            <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>r.pause(!t.paused)}><iconify-icon .icon=${t.paused?wt:$t} aria-hidden="true"></iconify-icon></button>
          </div>
          <button class="button" type="submit" aria-busy=${e?"true":"false"} ?disabled=${e||!s}>Save changes</button>
        </div>
      </form>
      <section class="history">
        <div class="history-head"><h3>Evaluation history</h3>${a.length?p`<span class="meta">Latest ${a.length}</span>`:f}</div>
        ${a.length?p`
          <div class="chart-plot">
            <div class="chart-scale" aria-hidden="true"><span>${h(l)}</span><span>${h(l/2)}</span><span>0 ms</span></div>
            <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${h(l)}`}>
              ${a.map(d=>{const x=d.succeeded?"Passed":"Failed",w=d.status_code===null?"network error":`HTTP ${d.status_code}`,C=c.get(d.executor_node_id)??`Node ${d.executor_node_id.slice(0,8)}`,$=`${x} at ${new Date(d.recorded_at_ms).toLocaleString()}: ${d.latency_ms} ms, ${w}. Executed by ${C}`;return p`<span class="history-bar ${d.succeeded?"up":"down"}" role="listitem" aria-label=${$} title=${$} style=${`height: ${Math.max(8,d.latency_ms/l*100)}%`}></span>`})}
            </div>
          </div>
          <div class="chart-axis"><span>${u(a[0].recorded_at_ms)}</span><span>${u(a.at(-1).recorded_at_ms)}</span></div>
          <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
        `:p`<p class="meta">No evaluations recorded yet.</p>`}
      </section>
    </dialog>`}var Ci=Object.getOwnPropertyDescriptor,Ai=(t,e,s,i)=>{for(var n=i>1?void 0:i?Ci(e,s):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(n=o(n)||n);return n};let Te=class extends wi{render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,s=this.alerts.filter(r=>r.delivery==="pending").length,i=["overview","alerts","cluster"],n=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name));return this.setupMode&&this.setup?p`
        <main class="shell setup-shell">
          <header>
            <div class="brand">
              <img src="/favicon.svg" alt="" />
              <div><div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"ready":"connecting"}</div></div><span>Distributed service monitoring</span></div>
            </div>
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ut[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error?p`<div class="notice" role="alert">${this.error}</div>`:f}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>`:p`
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
            ${i.map(r=>p`<a class=${this.activeSection===r?"active":""} href=${Le[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ut[this.theme]} aria-hidden="true"></iconify-icon></button>
          </div>
        </header>
        ${this.error?p`<div class="notice" role="alert">${this.error}</div>`:f}
        ${this.setup?.warning&&!this.warningDismissed?p`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:f}
        ${this.activeSection==="overview"?this.renderOverview(n,t,e,s):this.activeSection==="alerts"?_i(this.transitions,this.channels,{create:()=>this.openChannelDialog(),remove:r=>{this.deleteResource("channels",r.id,r.name)},setDefault:(r,o)=>{this.setChannelDefault(r,o)}}):this.renderClusterPage()}
      </main>
      ${ki(this.channels,this.saving,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeTargetDialog(),create:r=>{this.createTarget(r)}})}
      ${this.selected?Si(this.selected,this.saving,this.detailDirty,this.cluster?.members??[],this.channels,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),redirects:r=>this.toggleMaxRedirects(r),delete:()=>{this.deleteTarget()},pause:r=>{this.setPaused(r)}}):f}
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
          <label>Type<select name="type" @change=${r=>{this.channelKind=r.target.value,this.channelTestMessage=""}}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind==="webhook"?p`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" data-test-required required /></label>`:p`<label>Bot token<input name="bot_token" type="password" autocomplete="off" data-test-required required /></label><label>Chat ID<input name="chat_id" data-test-required required /></label>`}
          <label class="switch"><span>Default channel</span><input name="default" type="checkbox" role="switch" /></label>
          <div class="dialog-actions">${this.channelTestMessage?p`<span class="meta" role="status" style="margin-right:auto">${this.channelTestMessage}</span>`:f}<button class="button secondary" type="button" @click=${()=>this.closeDialog("channel-dialog")}>Cancel</button><button class="button secondary" type="button" aria-busy=${this.testingChannel} ?disabled=${this.testingChannel||this.saving} @click=${this.testChannel}>${this.testingChannel?"Sending…":"Send test"}</button><button class="button" type="submit" ?disabled=${this.saving||this.testingChannel}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create Join Token</h2><p>Choose how many days the token remains valid and whether it can be reused.</p></div>
        <form @submit=${this.createJoinToken}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required /></label>
          <label class="switch"><span>Unlimited uses</span><input type="checkbox" role="switch" .checked=${this.unlimitedUses} @change=${r=>this.unlimitedUses=r.target.checked} /></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join Token Created</h2><p>This command contains Cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderOverview(t,e,s,i){const n=this.targets.filter(a=>this.selectedIds.has(a.id)),r=n.some(a=>!a.paused),o=n.some(a=>a.paused);return p`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="overview-top">
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${i}</strong></div>
          <div class="metric"><span>Up</span><strong>${e}</strong></div>
          <div class="metric"><span>Down</span><strong>${s}</strong></div>
        </section>
        <section class="panel" aria-label="Secrets">
          <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length?this.secrets.map(a=>p`<div class="resource"><div><strong>${a.name}</strong><code>${a.id}</code></div><button class="button danger icon-button" aria-label=${`Delete secret ${a.name}`} title=${`Delete ${a.name}`} @click=${()=>this.deleteResource("secrets",a.id,a.name)}><iconify-icon .icon=${ae} aria-hidden="true"></iconify-icon></button></div>`):p`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${a=>this.search=a.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${a=>this.statusFilter=a.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${a=>this.sort=a.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?p`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${_t} aria-hidden="true"></iconify-icon></button>${r?p`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${$t} aria-hidden="true"></iconify-icon></button>`:f}${o?p`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${wt} aria-hidden="true"></iconify-icon></button>`:f}<button class="button danger icon-button" aria-label="Delete selected" title="Delete selected" @click=${this.bulkDelete}><iconify-icon .icon=${ae} aria-hidden="true"></iconify-icon></button></div></div>`:f}
        ${t.length?t.map(a=>this.renderTarget(a)):p`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
    `}renderClusterPage(){return p`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><h2>Nodes</h2><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(t=>p`<div class="resource"><div><strong>${t.name}</strong><code>${t.raft_url}</code></div><div class="actions">${t.local?p`<span class="badge">This node</span>`:f}${t.leader?p`<span class="badge">Leader</span>`:f}</div></div>`)}
        ${this.cluster?.members.length?f:p`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(t=>p`
              <div class="resource">
                <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
              </div>
            `):p`<div class="empty">No Join Tokens.</div>`}
      </section>
      </div>
    `}renderTarget(t){const e=t.latest_evaluation,s=t.history.slice(0,16).reverse(),i=Math.max(1,...s.map(r=>r.latency_ms)),n=t.paused?"paused":t.availability==="down"?"down":t.consecutive_failures>0?"suspicious":t.availability;return p`
      <div class="target-wrap">
        ${t.kind==="http"?p`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${r=>this.toggleSelected(t.id,r.target.checked)} />`:p`<span class="badge">Node</span>`}
        <button class=${`target ${t.kind==="node"?"node-target":""}`} aria-label=${t.name} @click=${t.kind==="http"?()=>this.openTarget(t):f}>
          <i class="state ${n}" aria-label=${n}></i>
          <div>
            <h3>${t.name}</h3>
            <div class="meta">${t.paused?"Paused · ":""}${t.method} · ${t.url} · every ${t.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${s.length?p`<div class="mini-chart" aria-hidden="true">${s.map(r=>p`<i class="mini-bar ${r.succeeded?"up":"down"}" style=${`height: ${Math.max(12,r.latency_ms/i*100)}%`}></i>`)}</div>`:f}
            <div class="latency">
              <strong>${e?`${e.latency_ms} ms`:"—"}</strong>
              <span>${e?t.kind==="node"?e.succeeded?"reachable":"unreachable":e.status_code??"network error":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};Te.styles=ft`
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
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 30px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 36px; height: 36px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .overview-top { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-bottom: 18px; }
    .page-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .summary { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
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
    .node-target { cursor: default; }
    .state { width: 10px; height: 10px; border-radius: 50%; color: var(--amber); background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
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
    .check { display: flex; align-items: center; gap: 8px; } .check input { width: auto; }
    .channel-fields { display: grid; gap: 7px; margin: 0; border: 0; padding: 0; }
    .channel-fields legend { margin-bottom: 5px; padding: 0; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    .channel-options { display: flex; flex-wrap: wrap; gap: 10px 16px; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .switch input { width: 40px; height: 22px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch input::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch input:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch input:checked::after { background: var(--button-text); transform: translateX(18px); }
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
      header { grid-template-columns: minmax(0, 1fr) auto; row-gap: 16px; }
      header > nav { display: flex; grid-column: 1 / -1; grid-row: 2; justify-self: center; }
      .overview-top { grid-template-columns: 1fr; }
      .page-columns { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .target-side { grid-column: 2; justify-self: start; }
      .latency { text-align: left; }
    }
  `;Te=Ai([yt("upgrid-app")],Te);
